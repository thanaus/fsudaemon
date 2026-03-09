# Analyse détaillée du projet fsudaemon — Bonnes pratiques 2026

> Rapport généré le 2026-03-08  
> Chaque fichier Python est analysé indépendamment selon les standards Python 3.12+, asyncio, Pydantic v2, et les pratiques de production 2026.

---

## Table des matières

1. [models.py](#1-modelspy)
2. [config.py](#2-configpy)
3. [audit_matcher.py](#3-audit_matcherpy)
4. [db_manager.py](#4-db_managerpy)
5. [sqs_consumer.py](#5-sqs_consumerpy)
6. [event_processor.py](#6-event_processorpy)
7. [telemetry.py](#7-telemetrypy)
8. [main.py](#8-mainpy)
9. [tools/init_db.py](#9-toolsinit_dbpy)
10. [tools/changelist.py](#10-toolschangelistpy)
11. [tools/sqs_ingest.py](#11-toolssqs_ingestpy)
12. [tools/db_inject.py](#12-toolsdb_injectpy)
13. [Synthèse globale](#13-synthèse-globale)

---

## 1. `models.py`

### Rôle
Définit les modèles de données Pydantic : `S3EventName` (enum), `AuditPoint`, `S3Event`.

### ✅ Points positifs

- **Pydantic v2 avec `ConfigDict(frozen=True)`** — les modèles sont immuables, ce qui est parfaitement adapté à des Value Objects qui ne doivent pas être mutés après création.
- **`S3EventName` comme `str, Enum`** — permet la sérialisation JSON native et la comparaison directe avec des strings, sans boilerplate.
- **`Field(ge=0)` sur `size`** — la validation côté Python est cohérente avec la contrainte `CHECK (size >= 0)` en base. Double filet de sécurité bien pensé.
- **`__repr__` personnalisé** sur les deux modèles — facilite le debugging sans exposer de données sensibles.
- **Enum exhaustif** pour `S3EventName` — couvre l'ensemble de la documentation AWS S3 event types, y compris les cas rares (Replication, IntelligentTiering, Lifecycle).

### ⚠️ Points à améliorer

#### 1.1 — `event_name: S3EventName` sans gestion du cas "inconnu"

```python
# Actuellement dans S3Event
event_name: S3EventName
```

Si AWS introduit un nouveau type d'événement non présent dans l'enum (ex: `ObjectCreated:RestoreCompleted`), Pydantic lèvera une `ValidationError` qui fera planter le traitement du message SQS entier. En production, cela crée un point de défaillance silencieux.

**Recommandation 2026 :** utiliser un type union avec un fallback, ou rendre l'enum permissif :

```python
# Option A — union avec str fallback
event_name: S3EventName | str

# Option B — Pydantic v2 : ignorer les valeurs inconnues
class S3EventName(str, Enum):
    _missing_ = classmethod(lambda cls, value: value)  # ne fonctionne pas avec Pydantic v2

# Option C — la plus robuste : use_enum_values + validator
class S3Event(BaseModel):
    model_config = ConfigDict(frozen=True, use_enum_values=True)
    event_name: str  # accepte tout, stocke la string brute
```

#### 1.2 — `audit_point_ids: list[int]` sans contrainte de taille

```python
audit_point_ids: list[int] = Field(default_factory=list)
```

Aucune limite supérieure sur le nombre d'audit points. Si un bucket a des centaines de prefixes imbriqués, un événement pourrait générer un tableau `INTEGER[]` massif en base. Cela peut impacter les performances des index GIN et la taille des rows.

**Recommandation :**

```python
audit_point_ids: list[int] = Field(default_factory=list, max_length=1000)
```

#### 1.3 — Pas de validation de `bucket` et `object_key`

Les champs `bucket` et `object_key` sont des strings sans contrainte. Un bucket vide (`""`) passerait la validation Pydantic et provoquerait un comportement indéfini dans `AuditPointMatcher`.

**Recommandation :**

```python
from pydantic import field_validator

@field_validator("bucket", "object_key")
@classmethod
def must_not_be_empty(cls, v: str) -> str:
    if not v:
        raise ValueError("must not be empty")
    return v
```

#### 1.4 — `version_id` accepte une string vide

```python
version_id: Optional[str] = None
```

AWS renvoie parfois `versionId: ""` (string vide) quand le versioning n'est pas activé. Stocker une string vide en base est différent de stocker `NULL`. Un validator devrait normaliser `""` → `None`.

```python
@field_validator("version_id", mode="before")
@classmethod
def empty_str_to_none(cls, v: str | None) -> str | None:
    return v or None
```

---

## 2. `config.py`

### Rôle
Chargement et validation de la configuration depuis `.env` via `pydantic-settings`. Point d'entrée unique pour tous les paramètres de l'application.

### ✅ Points positifs

- **`pydantic-settings` avec `SettingsConfigDict`** — standard 2026 pour la gestion de configuration, avec lecture automatique des variables d'environnement et du `.env`.
- **`SecretStr` pour les credentials** — les valeurs sensibles ne s'affichent pas dans `repr()` ni dans les logs. Appel explicite à `.get_secret_value()` requis.
- **Validateurs précis avec `@field_validator`** — chaque paramètre critique a sa plage de validité documentée et vérifiée (batch_size 1–10 conforme AWS SQS, wait_time 0–20, etc.).
- **`@model_validator(mode="after")`** pour les contraintes croisées — la validation `pool_min <= pool_max` ne peut pas être exprimée par un field_validator seul.
- **`_PROJECT_ROOT` pour localiser le `.env`** — le fichier est trouvé indépendamment du répertoire de travail courant, ce qui évite les surprises en production ou en CI.
- **`get_db_dsn()` avec `quote_plus`** — gestion correcte des caractères spéciaux dans user/password (espaces, `@`, etc.).

### ⚠️ Points à améliorer

#### 2.1 — `@lru_cache` rend les tests difficiles

```python
@lru_cache(maxsize=1)
def load_config() -> Config:
    return Config()
```

Le cache global de `lru_cache` persiste entre les tests si on ne l'efface pas explicitement. En pratique, cela contraint tous les tests qui nécessitent une config différente à appeler `load_config.cache_clear()`, ce qui est facilement oublié.

**Recommandation 2026 :** injecter la config explicitement dans les fonctions qui en ont besoin (déjà partiellement fait dans les tools), et réserver `load_config()` au `main()`. Alternativement, documenter la nécessité du `cache_clear()` dans une fixture pytest de base.

```python
# conftest.py
import pytest
from config import load_config

@pytest.fixture(autouse=True)
def clear_config_cache():
    load_config.cache_clear()
    yield
    load_config.cache_clear()
```

#### 2.2 — `db_port` sans validation de plage

```python
db_port: int
```

Aucune vérification que le port est dans la plage valide 1–65535. Un port à `0` ou à `-1` passerait sans erreur.

```python
@field_validator("db_port")
@classmethod
def validate_port(cls, v: int) -> int:
    if not 1 <= v <= 65535:
        raise ValueError(f"db_port must be between 1 and 65535, got {v}")
    return v
```

#### 2.3 — `otel_export_interval_seconds` non documenté dans `.env.example`

Le champ `otel_export_interval_seconds: int = 60` est présent dans `Config` mais absent du `.env.example`. Un opérateur qui voudrait ajuster l'intervalle d'export devra lire le code source pour connaître la variable d'environnement.

#### 2.4 — `aws_region` sans validation de format

```python
aws_region: str
```

N'importe quelle string est acceptée comme région. Une simple validation par pattern éviterait des erreurs AWS cryptiques au runtime :

```python
import re
_AWS_REGION_RE = re.compile(r"^[a-z]{2}-[a-z]+-\d$")

@field_validator("aws_region")
@classmethod
def validate_aws_region(cls, v: str) -> str:
    if not _AWS_REGION_RE.match(v):
        raise ValueError(f"Invalid AWS region format: {v!r}")
    return v
```

#### 2.5 — Pas de validation de l'URL SQS

`sqs_queue_url: str` accepte n'importe quelle string. Un validator minimal qui vérifie le format `https://sqs.<region>.amazonaws.com/<account_id>/<queue_name>` (ou `http://localhost:4566` pour LocalStack) éviterait des erreurs AWS tardives.

---

## 3. `audit_matcher.py`

### Rôle
Matching performant des événements S3 contre la liste des audit points (bucket + prefix). Noyau métier critique du daemon.

### ✅ Points positifs

- **Normalisation du prefix avec trailing `/`** — évite le faux positif classique où `data` matcherait `data-backup/file.txt`. Commentaire explicatif inclus, ce qui est rare et appréciable.
- **Assignation atomique de `_audit_points`** — l'ancien dict est remplacé en une seule opération, ce qui est safe sous asyncio (single-threaded). Le commentaire le précise explicitement.
- **Structure `dict[bucket → list[(id, prefix)]]`** — O(1) pour trouver les audit points du bon bucket, puis scan linéaire uniquement sur les prefixes du bucket concerné. Bon rapport complexité/lisibilité.
- **`get_stats()`** — méthode d'introspection utile pour les logs de démarrage et le monitoring.
- **`ValueError` avec messages explicites** — les erreurs de validation (bucket vide, prefix None) sont remontées avec contexte.

### ⚠️ Points à améliorer

#### 3.1 — Pas de type hint sur les annotations de classe

```python
_audit_points: dict[str, list[tuple[int, str]]]
_total_audit_points: int
```

Ces annotations de classe sont bien présentes, mais le type `dict[str, list[tuple[int, str]]]` est complexe à lire. Un `TypeAlias` améliorerait la lisibilité :

```python
from typing import TypeAlias

_BucketPrefixes: TypeAlias = list[tuple[int, str]]
_AuditPointIndex: TypeAlias = dict[str, _BucketPrefixes]
```

#### 3.2 — Scan linéaire non-optimisé pour les très grandes listes de prefixes

La méthode `get_matching_audit_points()` effectue un scan O(n) sur tous les prefixes d'un bucket. Pour un bucket avec 10 000 audit points (cas extrême), cela peut devenir un goulot.

En pratique, un trie (arbre de prefixes) permettrait un matching O(k) où k est la longueur de la clé. Pour les cas courants (< 1000 audit points), le scan linéaire actuel est tout à fait adapté, mais il n'y a pas de commentaire documentant cette limite.

#### 3.3 — `load_audit_points()` est publique mais écrase l'état en place

Si `load_audit_points()` est appelée pendant qu'une coroutine itère sur `_audit_points` (cas impossible sous asyncio monothreadé, mais possible si on passe à un executor), il n'y a pas de protection. Le commentaire "safe under asyncio single-thread execution" est présent, mais la méthode est publique sans garde-fou.

#### 3.4 — Pas de test unitaire pour les cas limites du matching

Les cas suivants sont critiques et devraient être couverts :
- prefix vide (match tout le bucket)
- prefix sans trailing `/` → normalisé
- clé identique au prefix (ex: prefix `data/`, clé `data/`) → doit matcher
- prefix `data` et clé `data-backup/file.txt` → ne doit PAS matcher
- bucket absent de l'index → retourne `[]`

---

## 4. `db_manager.py`

### Rôle
Gestion du pool de connexions PostgreSQL asyncpg, insertion des événements S3, chargement des audit points, et listener NOTIFY/LISTEN pour les changements d'audit points.

### ✅ Points positifs

- **`_MAX_BATCH_SIZE = 65535 // _INSERT_COLS`** — calcul explicite de la limite PostgreSQL (65535 paramètres max). Le découpage automatique en sous-batches dans `insert_events_batch()` est transparent pour l'appelant.
- **`ON CONFLICT DO NOTHING`** — idempotence correcte : les rejeux SQS ne créent pas de doublons. Cohérent avec l'index UNIQUE en base.
- **`max_inactive_connection_lifetime=300`** — recyclage des connexions inactives après 5 min, évite les connexions "zombies" perdues après un redémarrage réseau.
- **`_pending_tasks: set[asyncio.Task]`** — pattern correct Python 3.12+ pour éviter que le GC ne détruise une tâche asyncio avant sa complétion.
- **`call_soon_threadsafe`** — le callback NOTIFY d'asyncpg est synchrone ; l'utilisation de `call_soon_threadsafe` est la seule façon correcte de créer une tâche asyncio depuis ce contexte.
- **Classmethod `create()`** — factory pattern propre pour l'initialisation asynchrone (un `__init__` ne peut pas être `async`).

### ⚠️ Points à améliorer

#### 4.1 — Aucun retry sur `_insert_batch()`

```python
async with self.pool.acquire() as conn:
    await conn.execute(query, *args)
```

Si la connexion est perdue (redémarrage PostgreSQL, timeout réseau), l'exception remonte directement à l'appelant sans tentative de réessai. En production, une politique de retry avec backoff exponentiel est indispensable.

**Recommandation avec `tenacity` :**

```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    retry=retry_if_exception_type((asyncpg.PostgresConnectionError, OSError)),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=1, min=1, max=10),
)
async def _insert_batch(self, events: list[S3Event]) -> tuple[int, float]:
    ...
```

#### 4.2 — La connexion LISTEN est prélevée sur le pool de connexions

```python
self._conn = await self.db_manager.pool.acquire()
```

La connexion dédiée au LISTEN est acquise depuis le pool principal. Elle reste acquise indéfiniment (jusqu'à `stop_listening()`), ce qui réduit de 1 la disponibilité effective du pool pour les inserts. Sur un pool `min_size=5`, on perd 20% de la capacité minimale.

**Recommandation :** utiliser une connexion directe hors-pool :

```python
self._conn = await asyncpg.connect(self.db_manager._dsn)
```

Cela nécessite de stocker le DSN dans `DatabaseManager`, ce qui est un refactoring mineur.

#### 4.3 — `health_check()` insuffisant en production

```python
await conn.fetchval("SELECT 1")
```

`SELECT 1` vérifie que la connexion TCP est vivante, mais pas que les tables existent ou que l'utilisateur a les droits d'écriture. Un health check de production devrait vérifier au minimum que les tables critiques sont accessibles :

```python
await conn.fetchval("SELECT COUNT(*) FROM audit_points WHERE deleted_at IS NULL")
```

#### 4.4 — `_handle_notification` ignore les erreurs de désérialisation JSON silencieusement

```python
except Exception as e:
    logger.error("notification_handler_error", error=str(e), payload=payload)
```

L'exception est loguée mais aucun compteur de métriques n'est incrémenté. Une notification malformée (injection dans le payload NOTIFY, bug de trigger) ne déclenchera pas d'alerte monitoring.

**Recommandation :** incrémenter `instruments["errors"]` si disponible, ou exposer un compteur dédié.

#### 4.5 — `AuditPointsListener` ne gère pas la reconnexion automatique

Si la connexion PostgreSQL dédiée au LISTEN est perdue (timeout, redémarrage DB), le listener s'arrête silencieusement. Les changements d'audit points ne seront plus détectés jusqu'au redémarrage du daemon.

**Recommandation :** ajouter un watchdog qui détecte la perte de connexion et relance le listener :

```python
while not shutdown_event.is_set():
    try:
        await self.start_listening()
        await shutdown_event.wait()
    except Exception:
        await asyncio.sleep(5)  # backoff avant reconnexion
        continue
```

---

## 5. `sqs_consumer.py`

### Rôle
Consommateur SQS asynchrone via `aioboto3`. Gère la réception, la suppression en batch, et l'instrumentation des métriques.

### ✅ Points positifs

- **Client SQS persistant** — `start()`/`stop()` avec `__aenter__`/`__aexit__` évitent la création/destruction d'un client HTTP à chaque appel, ce qui améliore significativement les performances.
- **Long polling configuré** — `WaitTimeSeconds=20` réduit les requêtes à vide et les coûts SQS.
- **`AttributeNames=['All']`** — récupère tous les attributs SQS, utile pour le monitoring (ex: `ApproximateFirstReceiveTimestamp` pour mesurer la latence de traitement).
- **Instrumentation OTel** — les durées `receive_message` et `delete_message_batch` sont mesurées et exportées.
- **Batch delete en chunks de 10** — conforme à la limite AWS SQS.

### ⚠️ Points à améliorer

#### 5.1 — `receive_message()` sans guard sur `_client is None`

```python
async def receive_message(self) -> List[Dict[str, Any]]:
    response = await self._client.receive_message(...)
```

Si `start()` n'a pas été appelé, `self._client` est `None` et l'erreur sera `AttributeError: 'NoneType' object has no attribute 'receive_message'` — peu explicite pour un opérateur.

**Recommandation :**

```python
if self._client is None:
    raise RuntimeError("SQSConsumer not started. Call await consumer.start() first.")
```

#### 5.2 — `delete_message_batch()` retourne `deleted_count` partiel sans lever d'exception

```python
except Exception as e:
    logger.error("sqs_batch_delete_error", error=str(e))
    return deleted_count  # retourne 0 ou un nombre partiel
```

Si la suppression échoue, les messages redeviendront visibles dans SQS après le `visibility_timeout` et seront retraités. C'est le bon comportement (at-least-once delivery), mais le retour silencieux de `0` sans exception peut masquer un problème systémique.

**Recommandation :** lever l'exception après logging, ou au minimum incrémenter un compteur `errors` dans les instruments.

#### 5.3 — Pas de `VisibilityTimeout` extension pour les messages longs à traiter

Si le traitement d'un batch dépasse `sqs_visibility_timeout` (30s par défaut), SQS rendra les messages à nouveau visibles et ils seront traités en doublon. Pour des inserts PostgreSQL lents, ce cas peut survenir.

**Recommandation :** ajouter une méthode `extend_visibility_timeout()` appelable pendant le traitement, ou documenter clairement que `sqs_visibility_timeout` doit être supérieur au temps de traitement maximal estimé.

#### 5.4 — `Optional[str]` au lieu de `str | None` (style Python 3.10+)

```python
# Actuellement
aws_access_key_id: Optional[str] = None

# Style 2026 recommandé
aws_access_key_id: str | None = None
```

Le projet utilise `str | None` partout ailleurs (config.py, models.py). Cette incohérence de style devrait être corrigée (`ruff` le détecterait avec la règle `UP007`).

#### 5.5 — `List[Dict[str, Any]]` au lieu de `list[dict[str, Any]]`

```python
# Actuellement (typing legacy)
def receive_message(self) -> List[Dict[str, Any]]:

# Style Python 3.12 recommandé
def receive_message(self) -> list[dict[str, Any]]:
```

En Python 3.9+, les built-in types sont directement utilisables comme génériques. `List` et `Dict` de `typing` sont dépréciés depuis Python 3.9 et seront supprimés dans une future version.

---

## 6. `event_processor.py`

### Rôle
Orchestration du traitement des messages SQS : parsing des records S3, matching audit points, insertion en base, instrumentation.

### ✅ Points positifs

- **`unquote_plus()` sur les clés S3** — gestion correcte de l'URL-encoding AWS (espaces → `+`, caractères spéciaux → `%XX`). Commentaire explicatif inclus.
- **Séparation claire** entre `process_message()` (orchestration) et `_parse_s3_record()` (parsing unitaire) — facilite les tests unitaires du parsing.
- **Comptabilisation fine des métriques** — `events_kept`, `events_discarded`, `total_associations` sont tous instrumentés indépendamment.
- **Gestion des erreurs par record** — une exception sur un record n'interrompt pas le traitement des records suivants du même message.

### ⚠️ Points à améliorer

#### 6.1 — `S3EventName` peut lever `ValidationError` sur un event_name inconnu

```python
event = S3Event(
    event_name=event_name,  # string brute depuis le JSON SQS
    ...
)
```

Si `event_name` vaut `"ObjectCreated:RestoreCompleted"` (non présent dans l'enum), Pydantic v2 lèvera une `ValidationError`. Cette exception est attrapée par le `except Exception` du bloc parent et comptabilisée comme erreur — le message ne sera pas supprimé de SQS et sera retraité indéfiniment.

Voir recommandation 1.1 dans l'analyse de `models.py`.

#### 6.2 — `process_message()` retourne `errors` mais l'appelant l'utilise comme booléen

```python
# Dans main.py
errors = await processor.process_message(message)
if errors == 0:
    receipt_handles_to_delete.append(message['ReceiptHandle'])
```

Le contrat implicite est "0 erreur = succès = supprimer". Mais un message avec 9 records valides et 1 record malformé retourne `errors=1` et n'est PAS supprimé de SQS. Les 9 records valides seront réinsérés au prochain traitement (idempotence via `ON CONFLICT DO NOTHING`), mais cela génère du travail inutile.

**Recommandation :** distinguer les erreurs fatales (impossibilité de parser le body JSON) des erreurs partielles (un record malformé parmi plusieurs) :

```python
@dataclass
class ProcessingResult:
    should_delete: bool
    errors: int
    events_kept: int
```

#### 6.3 — `List`, `Dict` legacy dans les imports

```python
from typing import List, Dict, Any
```

Même remarque que pour `sqs_consumer.py` — utiliser les built-in types directement.

#### 6.4 — `event_time` fallback silencieux vers `now()`

```python
try:
    event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
except Exception:
    event_time = datetime.now(timezone.utc)
```

Si `eventTime` est absent ou malformé dans le record SQS, l'événement est quand même inséré avec l'heure courante. Cela fausse silencieusement la chronologie des événements en base. Un warning devrait être émis :

```python
except Exception:
    logger.warning("invalid_event_time", raw=event_time_str, fallback="now")
    event_time = datetime.now(timezone.utc)
```

#### 6.5 — Pas de validation que `bucket` et `key` sont non-vides avant le matching

```python
bucket = s3_data.get('bucket', {}).get('name', '')
key = unquote_plus(obj.get('key', ''))
```

Si `bucket` ou `key` est vide, `AuditPointMatcher.get_matching_audit_points()` lèvera un `ValueError`. Cette exception est bien attrapée, mais un log plus spécifique (`"missing_bucket_or_key"`) aiderait au diagnostic.

---

## 7. `telemetry.py`

### Rôle
Configuration OpenTelemetry avec export structlog (stdout) et OTLP optionnel. Définit les instruments (counters, histograms) utilisés dans tout le projet.

### ✅ Points positifs

- **`DELTA` temporality explicite** — les deltas sont calculés nativement par le SDK OTel, pas manuellement. Commentaire explicite sur la raison du choix.
- **`_METRIC_NAME_TO_KEY` mapping centralisé** — la correspondance entre noms OTel et clés structlog est déclarée en un seul endroit.
- **Support OTLP optionnel avec `ImportError` gracieux** — si le package `opentelemetry-exporter-otlp-proto-grpc` n'est pas installé, le daemon continue avec l'export stdout uniquement, sans crash.
- **`shutdown_metrics()`** — flush et shutdown propres à l'arrêt, évitant la perte des dernières métriques en cours d'export.
- **`instruments()` avec guard** — lève une `RuntimeError` explicite si appelée avant `init_metrics()`.

### ⚠️ Points à améliorer

#### 7.1 — Variables globales mutables

```python
_meter_provider: Optional[MeterProvider] = None
_metric_readers: list = []
_instruments: Optional[dict] = None
```

Ces globals mutables rendent le module non-réentrant et difficile à tester (état partagé entre tests). En 2026, le pattern recommandé est un objet `TelemetryContext` injectable :

```python
@dataclass
class TelemetryContext:
    meter_provider: MeterProvider
    instruments: dict[str, Any]

    def shutdown(self) -> None: ...
```

#### 7.2 — `_instruments` typé `Optional[dict]` trop large

```python
_instruments: Optional[dict] = None
```

Le type `dict` sans paramètre n'indique pas quelles clés sont attendues ni quels types d'instruments elles contiennent. Un `TypedDict` rendrait l'usage de `instruments()` plus sûr :

```python
from typing import TypedDict
from opentelemetry.sdk.metrics import Counter, Histogram

class FsuInstruments(TypedDict):
    messages_received: Counter
    messages_processed: Counter
    events_kept: Counter
    events_discarded: Counter
    total_associations: Counter
    errors: Counter
    db_insert_seconds: Histogram
    sqs_receive_message_seconds: Histogram
    sqs_delete_message_batch_seconds: Histogram
```

#### 7.3 — `_extract_metrics_payload()` : accès par `hasattr` fragile

```python
if hasattr(data_points[0], "value"):
    total = sum(getattr(dp, "value", 0) or 0 for dp in data_points)
elif hasattr(data_points[0], "sum"):
    ...
```

Cette détection duck-typing des types de data points (Counter vs Histogram) est fragile face aux évolutions de l'API OTel SDK. Une vérification sur le type du `data` object serait plus robuste :

```python
from opentelemetry.sdk.metrics.export import (
    Sum, Histogram as HistogramData
)

if isinstance(metric.data, Sum):
    ...
elif isinstance(metric.data, HistogramData):
    ...
```

#### 7.4 — `export_interval_seconds` non validé dans `init_metrics()`

```python
def init_metrics(export_interval_seconds: int = 60, ...):
```

La validation de la plage (1–3600) est faite dans `Config` mais pas dans `init_metrics()` elle-même, qui est une API publique. Un appel direct à `init_metrics(export_interval_seconds=0)` créerait un reader qui exporte en boucle infinie.

---

## 8. `main.py`

### Rôle
Point d'entrée du daemon. Orchestre le démarrage, la configuration, les workers asyncio, et l'arrêt gracieux.

### ✅ Points positifs

- **Ordre de démarrage bien documenté** — les 10 étapes (config → logging → telemetry → DB → audit points → SQS → processor → signals → workers → wait) sont numérotées et commentées.
- **`setup_logging()` séparé de la configuration** — le logging est reconfiguré avec le niveau issu de la config, après que la config a été chargée.
- **`shutdown_event = asyncio.Event()`** — pattern propre pour la coordination de l'arrêt entre coroutines.
- **`signal_handler` pour SIGINT/SIGTERM** — gestion des deux signaux d'arrêt standard. Crucial pour les déploiements Kubernetes.
- **`asyncio.gather(*tasks, return_exceptions=True)`** — attend la fin de toutes les tâches sans propager les `CancelledError`, ce qui permet un arrêt propre.
- **Warning si aucun audit point** — prévient l'opérateur que tous les messages seront jetés, au lieu de démarrer silencieusement.

### ⚠️ Points à améliorer

#### 8.1 — `shutdown_event` est un global module-level

```python
shutdown_event = asyncio.Event()
```

Ce global crée une dépendance implicite entre `main()`, `process_messages_loop()`, `listen_audit_points_changes()`, et `signal_handler()`. En 2026, il est préférable de passer explicitement le `shutdown_event` en paramètre à chaque fonction qui l'utilise, ce qui rend les dépendances visibles et facilite les tests.

#### 8.2 — `process_messages_loop()` : backoff exponentiel recalculé depuis 1 à chaque erreur

```python
await asyncio.sleep(min(2 ** consecutive_errors, 60))
```

Ce backoff est correct, mais il repart de 1 dès que `consecutive_errors` est remis à 0 après un succès. Un opérateur qui observe des erreurs intermittentes verra des délais variables imprévisibles. Documenter ce comportement dans les logs serait utile :

```python
logger.info("backoff_sleep", seconds=min(2 ** consecutive_errors, 60))
```

#### 8.3 — Pas de timeout sur la phase d'arrêt

```python
await asyncio.gather(*tasks, return_exceptions=True)
await sqs_consumer.stop()
await db_manager.pool.close()
```

Si une tâche ne se termine pas dans un délai raisonnable (ex: `listen_audit_points_changes` bloquée), le shutdown peut durer indéfiniment. En production, Kubernetes envoie un SIGKILL après `terminationGracePeriodSeconds`.

**Recommandation :**

```python
try:
    await asyncio.wait_for(
        asyncio.gather(*tasks, return_exceptions=True),
        timeout=30.0
    )
except asyncio.TimeoutError:
    logger.warning("shutdown_timeout", message="Tasks did not finish in 30s, forcing exit")
```

#### 8.4 — `parse_args()` sans utilisation de son résultat

```python
def parse_args():
    parser = argparse.ArgumentParser(description='S3 Event Processor')
    return parser.parse_args()

# Dans main()
parse_args()  # résultat ignoré
```

La fonction est appelée mais son résultat est ignoré. Si l'on ajoute un argument CLI à l'avenir (ex: `--config-file`), il faudra penser à capturer le résultat. Actuellement, cette fonction sert uniquement à afficher `--help`, ce qui devrait être documenté.

#### 8.5 — Logging de démarrage avant `setup_logging()`

```python
logger.info("s3_event_processor_starting")  # logger structlog non encore configuré

config = load_config()
setup_logging(config.log_level)             # configuration réelle ici
```

Le premier `logger.info` est émis avec le logger bootstrap (format `%(message)s` stdlib), pas avec structlog JSON. Ce n'est pas un bug, mais la première ligne de log sera dans un format différent du reste, ce qui peut perturber les parsers de logs.

---

## 9. `tools/init_db.py`

### Rôle
Script d'initialisation du schéma PostgreSQL (tables, indexes, trigger NOTIFY). Idempotent via `CREATE IF NOT EXISTS`.

### ✅ Points positifs

- **Transaction unique pour toutes les DDL** — si un step échoue, tout le schéma est rollback. Cohérence garantie, pas de schéma partiellement initialisé.
- **`DROP TRIGGER IF EXISTS` avant `CREATE TRIGGER`** — gestion correcte de la ré-exécution idempotente du script.
- **Index UNIQUE pour l'idempotence** — `(bucket, object_key, event_time, event_name)` cohérent avec `ON CONFLICT DO NOTHING` dans l'insert.
- **Index GIN** pour les requêtes `@>` sur `audit_point_ids INTEGER[]` — choix correct pour les tableaux PostgreSQL.
- **Index partiel sur `deleted_at`** — `WHERE deleted_at IS NULL` réduit la taille de l'index et améliore les performances des requêtes sur les audit points actifs.
- **Statistiques post-initialisation** — affichage du nombre d'objets existants, utile pour valider une ré-exécution.

### ⚠️ Points à améliorer

#### 9.1 — Connexion directe sans pool, jamais fermée en cas d'exception

```python
conn = await asyncpg.connect(config.get_db_dsn())
try:
    ...
except Exception as e:
    print(f"❌ Initialization error: {e}")
    raise
finally:
    await conn.close()
```

Le `finally` garantit la fermeture, mais si `asyncpg.connect()` lui-même échoue, `conn` sera non défini et le `finally` lèvera un `UnboundLocalError`. La bonne pratique :

```python
async with await asyncpg.connect(config.get_db_dsn()) as conn:
    ...
```

#### 9.2 — Pas de logging structuré — utilise `print()`

Ce script utilise `print()` plutôt que `structlog`. Pour un outil de production, les logs structurés sont préférables même dans les scripts (facilite la supervision CI/CD).

#### 9.3 — Le trigger NOTIFY ne protège pas `NEW.prefix` contre `NULL`

```sql
'prefix', COALESCE(NEW.prefix, OLD.prefix)
```

Ce COALESCE est correct pour les DELETE où NEW est NULL, mais si un UPDATE set `prefix = NULL` explicitement (cas pathologique), `COALESCE(NEW.prefix, OLD.prefix)` retournera l'ancienne valeur sans erreur visible. Une contrainte `NOT NULL` sur la colonne `prefix` serait plus sûre.

#### 9.4 — Absence de migration versionnée

En 2026, un script `init_db.py` monolithique avec `IF NOT EXISTS` est insuffisant pour gérer l'évolution du schéma en production. L'adoption d'un outil de migration comme **Alembic** ou **Flyway** est recommandée, permettant des rollbacks et un historique des changements.

---

## 10. `tools/changelist.py`

### Rôle
Script CLI de récupération de tous les événements entre deux audit points, avec pagination par keyset.

### ✅ Points positifs

- **Pagination keyset (`id > $3 ORDER BY id`)** — O(log n) vs O(n²) d'un OFFSET classique. Indispensable pour les grandes tables.
- **`BatchHandler` type alias** — documenter le type du callback via un `TypeAlias` est une bonne pratique qui améliore la lisibilité et l'autocomplétion IDE.
- **Validation `limit_date > start_date`** — prévient l'inversion des audit points avec un message d'erreur explicite.
- **Injection de dépendances** — `config`, `logger`, et `row_handler` sont injectés, pas récupérés depuis des globals. Rend la fonction testable.
- **`_noop_handler` par défaut** — l'interface est utilisable sans handler, même si cela peut induire en erreur (voir ci-dessous).

### ⚠️ Points à améliorer

#### 10.1 — `_noop_handler` par défaut sans warning à l'exécution

```python
async def _noop_handler(rows: Sequence[asyncpg.Record]) -> None:
    """Default handler — does nothing. Replace with your own implementation."""
    pass
```

Un utilisateur qui oublie de fournir un `row_handler` au script CLI obtient silencieusement 0 résultat exploité. Un warning à l'exécution CLI serait utile :

```python
if row_handler is _noop_handler:
    log.warning("noop_handler_active", message="No row_handler provided — results will be discarded")
```

#### 10.2 — `type BatchHandler = ...` (PEP 695 syntax)

```python
type BatchHandler = Callable[[Sequence[asyncpg.Record]], Awaitable[None]]
```

La syntaxe `type X = ...` (PEP 695, Python 3.12) est utilisée ici, mais le reste du projet utilise `TypeAlias` de `typing`. Il faudrait homogénéiser. La syntaxe PEP 695 est la plus moderne, mais elle est moins lisible pour des développeurs venant de Python < 3.12.

#### 10.3 — Pas de compteur de progression pour les très grandes changelists

Sur une table avec des millions d'événements, le script peut tourner plusieurs heures sans sortie entre les batchs (sauf les logs `changelist_batch`). Ajouter un ETA ou un taux de progression serait utile.

#### 10.4 — La requête SQL inclut `SELECT *` 

```sql
SELECT *
FROM s3_events
WHERE audit_point_ids @> ARRAY[$1::integer]
  AND received_at < $2
  AND id > $3
ORDER BY id
LIMIT $4
```

`SELECT *` est acceptable pour un outil CLI, mais si la table évolue (ajout de colonnes), le `row_handler` de l'utilisateur peut recevoir des colonnes inattendues. Lister les colonnes explicitement serait plus robuste.

---

## 11. `tools/sqs_ingest.py`

### Rôle
Script CLI d'injection de faux événements S3 dans une queue SQS, pour les tests de charge et de développement.

### ✅ Points positifs

- **Batch SQS de 10** — conforme à la limite AWS `SendMessageBatch`.
- **Gestion des échecs partiels** — les `Failed` de l'API batch sont logués avec les détails.
- **`run_ingest()` séparé de `main()`** — la logique métier est testable sans parser les arguments CLI.
- **`sys.exit(1)` si des messages échouent** — signale l'échec aux scripts CI/CD.

### ⚠️ Points à améliorer

#### 11.1 — `generate_s3_events()` génère des événements avec timestamps identiques

```python
'eventTime': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
```

Tous les événements d'une même exécution auront un `eventTime` identique à la milliseconde près. Avec l'index UNIQUE `(bucket, object_key, event_time, event_name)` en base, cela peut causer des conflits inattendus lors des tests, surtout avec des noms de fichiers générés de façon non-unique.

**Recommandation :** varier légèrement l'`eventTime` :

```python
from datetime import timedelta
import random

'eventTime': (datetime.now(UTC) - timedelta(seconds=random.randint(0, 3600))).isoformat(...)
```

#### 11.2 — `versionId: None` sérialisé comme `null` en JSON

```python
'versionId': f'v{random.randint(1, 100)}' if random.random() > 0.3 else None,
```

La sérialisation `json.dumps({'versionId': None})` produit `"versionId": null`. L'event_processor gère ce cas via `obj.get('versionId')`, mais un vrai événement AWS S3 n'inclut pas le champ `versionId` quand le versioning n'est pas activé — il est absent du JSON, pas `null`. Le générateur devrait reproduire ce comportement.

#### 11.3 — Pas de `--dry-run` option

Pour valider la génération d'événements sans les envoyer à SQS, une option `--dry-run` serait utile en développement.

---

## 12. `tools/db_inject.py`

### Rôle
Script CLI d'injection directe de faux événements S3 en base PostgreSQL, sans passer par SQS. Utilisé pour les benchmarks.

### ✅ Points positifs

- **Réutilisation de `AuditPointMatcher`** — les événements injectés passent par le vrai matcher, ce qui donne des résultats réalistes pour les benchmarks.
- **Métriques de performance** — `events_per_second` calculé et logué à la fin, directement exploitable pour des comparaisons de benchmarks.
- **`run_inject()` injecte ses dépendances** — testable unitairement.
- **Batch size configurable** — `--batch-size` avec validation (1–10000).

### ⚠️ Points à améliorer

#### 12.1 — `dicts_to_s3_events()` duplique la logique de `EventProcessor._parse_s3_record()`

Les deux fonctions font la même chose : extraire `bucket`, `key`, `size`, `version_id`, `event_name`, `event_time` depuis un dict d'événement S3, puis appeler `matcher.get_matching_audit_points()`. Cette duplication est un risque de dérive : si le format du message S3 évolue, il faudra mettre à jour les deux endroits.

**Recommandation :** extraire une fonction utilitaire partagée dans un module `parsers.py` ou exposer `_parse_s3_record` comme fonction publique.

#### 12.2 — `generate_s3_event_dicts()` vs `generate_s3_events()` dans sqs_ingest.py

Les deux scripts ont chacun leur propre générateur d'événements avec des structures légèrement différentes. La même recommandation s'applique : centraliser dans un module `fixtures.py` ou `testing/generators.py`.

#### 12.3 — Pas de validation que des audit points existent avant l'injection

```python
audit_points = await db.load_audit_points()
matcher = AuditPointMatcher(audit_points)
log.info("audit_points_loaded", count=len(audit_points))
```

Si aucun audit point n'est configuré, tous les événements générés auront `audit_point_ids=[]` et seront stockés en base sans association. Le benchmark mesurera les inserts mais pas le matching. Un warning similaire à celui de `main.py` devrait être émis.

---

## 13. Synthèse globale

### Tableau de scoring par fichier

| Fichier | Qualité | Tests manquants | Dettes principales |
|---|---|---|---|
| `models.py` | ⭐⭐⭐⭐ | Validators edge cases | `S3EventName` non-permissif, pas de validation `bucket`/`key` |
| `config.py` | ⭐⭐⭐⭐ | Config avec valeurs limites | `lru_cache` difficile à tester, `db_port` non validé |
| `audit_matcher.py` | ⭐⭐⭐⭐ | Tests critiques absents | Pas de TypeAlias, limite non documentée sur N prefixes |
| `db_manager.py` | ⭐⭐⭐⭐ | Tests intégration | Pas de retry, connexion LISTEN sur pool, reconnexion absente |
| `sqs_consumer.py` | ⭐⭐⭐ | Tests unitaires | Guard `_client is None`, types legacy `List`/`Dict` |
| `event_processor.py` | ⭐⭐⭐ | Tests unitaires/intégration | `S3EventName` ValidationError non gérée, duplication parsing |
| `telemetry.py` | ⭐⭐⭐⭐ | Tests d'export | Globals mutables, duck-typing fragile |
| `main.py` | ⭐⭐⭐⭐ | Tests E2E | `shutdown_event` global, pas de timeout arrêt |
| `tools/init_db.py` | ⭐⭐⭐ | — | `print()` au lieu de logging, pas de migrations |
| `tools/changelist.py` | ⭐⭐⭐⭐ | Tests intégration | `noop_handler` silencieux, `SELECT *` |
| `tools/sqs_ingest.py` | ⭐⭐⭐ | — | Timestamps identiques, duplication générateur |
| `tools/db_inject.py` | ⭐⭐⭐ | — | Duplication parsing/générateur, pas de warning sans audit points |

### Priorités de remédiation

**P0 — Risque production immédiat**
1. Gérer `ValidationError` sur `S3EventName` inconnu dans `event_processor.py` — peut bloquer des messages SQS indéfiniment.
2. Ajouter un retry sur `_insert_batch()` dans `db_manager.py` — les erreurs transitoires PostgreSQL causent des pertes de données.
3. Gérer la reconnexion automatique de `AuditPointsListener` — une perte de connexion PostgreSQL désactive silencieusement la synchronisation des audit points.

**P1 — Qualité et maintenabilité**
4. Écrire des tests unitaires pour `AuditPointMatcher` et `EventProcessor._parse_s3_record()`.
5. Utiliser une connexion hors-pool pour le LISTEN dans `AuditPointsListener`.
6. Ajouter un timeout sur la phase de shutdown dans `main.py`.
7. Corriger les types legacy `List`/`Dict`/`Optional` → `list`/`dict`/`X | None`.

**P2 — Améliorations futures**
8. Introduire Alembic pour les migrations de schéma.
9. Ajouter un `Dockerfile` multi-stage non-root.
10. Introduire `uv.lock` pour le lock des dépendances.
11. Centraliser les générateurs d'événements de test (`sqs_ingest.py` / `db_inject.py`).
12. Passer `shutdown_event` en paramètre plutôt qu'en global.
