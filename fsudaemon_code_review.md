# Code Review — fsudaemon
## Analyse détaillée · Bonnes pratiques Python 2026

> Pipeline `SQS → Audit Matching → PostgreSQL`  
> 9 fichiers Python analysés · Python 3.12 · asyncio · asyncpg · aioboto3 · pydantic-settings · OpenTelemetry  
> Mars 2026 — v4 (post-corrections)

---

## Corrections appliquées

| # | Fichier | Correction |
|---|---|---|
| 1 | `main.py` | Logs de cycle de vie promus `debug` → `info` |
| 2 | `main.py` | `aws_access_key_id` unwrappé via `.get_secret_value()` |
| 3 | `main.py` | `log_level` Config appliqué dynamiquement via `setup_logging()` |
| 4 | `main.py` | `otel_export_interval_seconds` lu depuis la config (plus hardcodé) |
| 5 | `config.py` | Ajout de `otel_export_interval_seconds` avec validator (1-3600) |
| 6 | `event_processor.py` | URL-decode des clés S3 via `unquote_plus()` |
| 7 | `sqs_consumer.py` | `get_queue_attributes()` supprimée (non utilisée) |
| 8 | `db_manager.py` | `insert_events_batch` subdivisé pour respecter la limite 65535 params PostgreSQL |
| 9 | `config.py` | Validation `db_host` via regex hostname/IPv4 |
| 10 | `models.py` | Migration `@dataclass` → Pydantic v2 `BaseModel` (`frozen=True`, `Field(ge=0)`) |

---

## 1. Tableau de bord global

| Fichier | Score v1 | Score v2 | Score v3 | Priorité | Statut |
|---|---|---|---|---|---|
| `config.py` | 9/10 | 9/10 | 10/10 | 🟢 Faible | Validators complets. `db_host` validé. `otel_export_interval_seconds` ajouté. |
| `models.py` | 7/10 | 7/10 | 9/10 | 🟢 Faible | Migré vers Pydantic v2 `BaseModel`. `frozen=True`, `Field(ge=0)`. |
| `audit_matcher.py` | 8/10 | 8/10 | 8/10 | 🟢 Faible | Logique claire, normalisation prefix solide. |
| `db_manager.py` | 7/10 | 7/10 | 8/10 | 🟡 Moyenne | Limite 65535 corrigée. Retry DB toujours ouvert. |
| `sqs_consumer.py` | 8/10 | 9/10 | 9/10 | 🟢 Faible | API propre. Inchangé depuis v2. |
| `event_processor.py` | 7/10 | 8/10 | 8/10 | 🟢 Faible | URL-decode corrigé. Inchangé depuis v2. |
| `telemetry.py` | 8/10 | 8/10 | 8/10 | 🟢 Faible | OTel bien intégré. Inchangé. |
| `main.py` | 7/10 | 9/10 | 10/10 | 🟢 Faible | `log_level` dynamique, plus aucune valeur hardcodée. |
| `tools/*.py` | 7/10 | 7/10 | 7/10 | 🟡 Moyenne | Bons outils. Config injectée. Pagination keyset OK. |

**Légende priorité :** 🔴 Critique = blocker production · 🟠 Élevée = corriger sous 2 semaines · 🟡 Moyenne = dette technique · 🟢 Faible = amélioration continue

---

## 2. config.py

**Rôle :** Chargement et validation de la configuration applicative depuis les variables d'environnement et le fichier `.env`, via `pydantic-settings`.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | `pydantic-settings` v2 | `BaseSettings` avec `SettingsConfigDict` est l'approche recommandée en 2026. |
| ✅ | `SecretStr` sur tous les credentials | `aws_access_key_id`, `aws_secret_access_key` et `db_password` typés `SecretStr`. La valeur n'apparaît jamais dans les logs, repr ou tracebacks. |
| ✅ | Validators stricts et complets | Chaque paramètre sensible a son `@field_validator` avec bornes explicites. Erreurs détectées au démarrage. |
| ✅ | `lru_cache` sur `load_config()` | Évite de relire le `.env` à chaque appel. Pattern standard pour les singletons de config. |
| ✅ | Résolution `_PROJECT_ROOT` | `Path(__file__).resolve().parent` garantit que le `.env` est trouvé indépendamment du CWD. |
| ✅ | `@model_validator` sur pool sizes | Validation croisée `db_pool_min_size <= db_pool_max_size` au niveau modèle. |
| ✅ | `otel_export_interval_seconds` configurable | Valeur par défaut `60`, exposée dans `.env`, validée entre 1 et 3600. Plus aucune valeur OTel hardcodée. |
| ✅ | Validation `db_host` | Regex hostname et IPv4 : rejette les valeurs vides, les espaces, les formats invalides. Erreur détectée au démarrage. |

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Lisibilité & structure | 5/5 | Code auto-documenté, noms explicites, organisation claire. |
| Sécurité credentials | 5/5 | `SecretStr` sur tous les credentials, unwrap correctement géré. |
| Robustesse validation | 5/5 | Tous les paramètres critiques validés, y compris `db_host`. |
| Idiomes Python 2026 | 5/5 | `pydantic-settings` v2, `lru_cache`, `SecretStr`, union types `X \| Y`. |

**Score global : 10/10**

---

## 3. models.py

**Rôle :** Définition des structures de données du domaine : `AuditPoint` et `S3Event`.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | Pydantic v2 `BaseModel` | Les deux modèles héritent de `BaseModel`. Validation automatique des types, sérialisation JSON native, coercion des `datetime` depuis `str`. |
| ✅ | `frozen=True` sur les deux modèles | `ConfigDict(frozen=True)` rend les instances immuables après création. Mutations accidentelles détectées à l'exécution. |
| ✅ | `Field(ge=0)` sur `S3Event.size` | Valeurs négatives rejetées au niveau Python, en cohérence avec la contrainte `CHECK (size >= 0)` en DB. |
| ✅ | `Field(default_factory=list)` | Évite le classique bug Python du mutable default argument. |
| ✅ | `__repr__` informatifs conservés | Les repr personnalisés incluent l'état métier. Plus lisibles que le repr automatique Pydantic dans les logs. |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| ⚠️ | `event_name` non validé | `event_name` accepte n'importe quelle chaîne. Un `Literal["ObjectCreated:Put", "ObjectCreated:Post", ...]` ou un `Enum` renforcerait le contrat. |

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Correction des types | 5/5 | Pydantic v2 `BaseModel` avec validation automatique. |
| Immutabilité & sécurité | 5/5 | `frozen=True` et `Field(ge=0)` sur les deux modèles. |
| Idiomes Python 2026 | 5/5 | `BaseModel`, `ConfigDict`, `Field` : stack Pydantic v2 moderne. |
| Lisibilité | 5/5 | `__repr__` soignés conservés, noms expressifs, structure claire. |

**Score global : 9/10**

---

## 4. audit_matcher.py

**Rôle :** Moteur de matching en mémoire : détermine quels audit points correspondent à un event S3 (bucket + key). Rechargeable à chaud via NOTIFY PostgreSQL.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | `dict[bucket] → list[(id, prefix)]` | Partitionnement par bucket O(1) avant la boucle de préfixes. Optimal pour un grand nombre de buckets. |
| ✅ | Normalisation des préfixes | L'ajout de `/` en suffixe empêche les faux positifs (`data/` ne matche pas `data-backup/file.txt`). |
| ✅ | Assignation atomique | `self._audit_points = new_audit_points` en une seule assignation. Correct pour le single-thread asyncio. |
| ✅ | Validation d'entrée dans `get_matching` | `ValueError` explicite si `bucket` ou `key` est vide/non-str. Contrat d'interface bien défini. |
| ✅ | `get_stats()` pour l'observabilité | Utile pour les logs de démarrage et le debugging en production. |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| ⚠️ | Pas de tests unitaires | La logique de normalisation et de matching est critique : préfixe vide, préfixe exact, sous-répertoires, collision (`data` vs `data-backup`). Ces cas méritent une suite de tests. |
| ⚠️ | `ValueError` non catchée dans le caller | Si un `AuditPoint` a un bucket vide, la `ValueError` n'est pas catchée dans `sync_audit_points()`. Elle pourrait stopper silencieusement le listener NOTIFY. |

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Algorithme & performance | 5/5 | Dict bucket O(1) + `startswith`. Optimal pour ce use case. |
| Robustesse & edge cases | 3/5 | Normalisation prefix bonne, mais `ValueError` non catchée dans le caller. |
| Testabilité | 3/5 | Classe pure, facile à tester, mais aucun test fourni. |
| Lisibilité | 5/5 | Commentaires détaillés, noms explicites, logique bien structurée. |

**Score global : 8/10**

---

## 5. db_manager.py

**Rôle :** Gestion du pool de connexions asyncpg, chargement des audit points, insertion batch des events S3, écoute des notifications LISTEN/NOTIFY PostgreSQL.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | Factory `@classmethod async def create()` | Pattern idiomatique pour les constructeurs asynchrones en Python. |
| ✅ | `ON CONFLICT DO NOTHING` pour l'idempotence | Garantit qu'un replay SQS n'insère pas de doublons. |
| ✅ | `max_inactive_connection_lifetime=300` | Recyclage des connexions idle après 5 min. |
| ✅ | Subdivision automatique des batches | `insert_events_batch` délègue à `_insert_batch` et subdivise si `len(events) > 9362` pour rester sous la limite des 65535 paramètres PostgreSQL. |
| ✅ | `_INSERT_COLS` et `_MAX_BATCH_SIZE` comme constantes | Le calcul `65535 // 7 = 9362` est explicite et se recalcule automatiquement si une colonne est ajoutée. |
| ✅ | LISTEN/NOTIFY avec `_pending_tasks` | Conservation des références de tasks pour éviter le garbage collection Python 3.12+. |
| ✅ | `call_soon_threadsafe` + `_schedule` | Seule façon correcte de planifier une coroutine depuis un thread externe (callback asyncpg). |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| 🔴 | Pas de retry sur les erreurs DB transitoires | Une coupure réseau brève remonte dans `consecutive_errors`. Après 5 erreurs, le daemon s'arrête. Un retry avec backoff exponentiel éviterait les faux arrêts. |
| ⚠️ | `health_check()` pas appelé périodiquement | Effectué uniquement au démarrage. En production, le pool peut perdre toutes ses connexions sans détection. |
| ⚠️ | Connexion non libérée en cas d'échec dans `start_listening` | Si `add_listener()` échoue après `acquire()`, la connexion n'est pas relâchée. Un `try/finally` corrigerait la fuite. |

### Correction recommandée — fuite connexion dans `start_listening`

```python
async def start_listening(self) -> None:
    self._loop = asyncio.get_running_loop()
    self._conn = await self.db_manager.pool.acquire()
    try:
        await self._conn.add_listener('audit_points_changed', self._handle_notification)
    except Exception:
        await self.db_manager.pool.release(self._conn)
        self._conn = None
        raise
    self._listening = True
```

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Patterns async asyncpg | 5/5 | `create()`, `pool.acquire()`, LISTEN/NOTIFY : tous idiomatiques. |
| Robustesse & résilience | 3/5 | Limite 65535 corrigée. Pas de retry DB transitoire. |
| Idempotence & cohérence | 5/5 | `ON CONFLICT DO NOTHING` avec index UNIQUE : parfait. |
| Gestion ressources | 3/5 | Connexion potentiellement leakée dans `start_listening()`. |

**Score global : 8/10**

---

## 6. sqs_consumer.py

**Rôle :** Consumer SQS asynchrone avec client persistant, long polling, suppression en batch, et mesure de latence via OpenTelemetry.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | Client persistant via `__aenter__` | Évite la reconnexion HTTP/TLS à chaque `receive_message`. Crucial pour les performances. |
| ✅ | Long polling `WaitTimeSeconds` | Pratique recommandée AWS. Réduit le nombre d'appels API et les coûts. |
| ✅ | `delete_message_batch` par lots de 10 | Respect de la limite SQS. Gère correctement les batches partiels. |
| ✅ | Instrumentation OTel des appels SQS | Mesure de latence sur `receive_message` et `delete_message_batch` via histogrammes. |
| ✅ | `endpoint_url` pour LocalStack | Support natif de LocalStack. Facilite le développement local. |
| ✅ | API surface réduite | `get_queue_attributes()` supprimée. La classe n'expose que ce qu'elle implémente réellement. |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| ⚠️ | Pas de reconnexion automatique | Si le client SQS perd sa connexion (timeout réseau, rotation IAM), `_client` reste en état d'erreur. |
| ⚠️ | `AttributeNames=['All']` systématique | Récupérer tous les attributs augmente la taille des messages. Spécifier uniquement les attributs utilisés réduirait les coûts. |

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Patterns async aioboto3 | 5/5 | Client persistant, context manager, long polling : parfait. |
| Sécurité credentials | 5/5 | `SecretStr` correctement unwrappé avant passage au consumer. |
| Robustesse réseau | 3/5 | Pas de reconnexion automatique sur perte de connexion. |
| Observabilité & API | 5/5 | Histogrammes OTel complets. API propre sans méthodes mortes. |

**Score global : 9/10**

---

## 7. event_processor.py

**Rôle :** Orchestration du traitement : parse les messages SQS, délègue le matching aux audit points, gère les métriques, insère en base les events pertinents.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | Séparation `process_message` / `_parse_s3_record` | Orchestration vs parsing : lisible et testable indépendamment. |
| ✅ | Comptage granulaire des erreurs | `errors` compté par record. Un message avec 5 records dont 1 défectueux est correctement géré. |
| ✅ | Guard `if events_to_insert` | Évite un appel DB inutile quand aucun event ne matche. |
| ✅ | `event_time` avec fallback UTC | En cas de timestamp malformé, `datetime.now(timezone.utc)` est utilisé. Comportement dégradé sûr. |
| ✅ | URL-decode des clés S3 | `unquote_plus()` appliqué sur les object keys. Les clés stockées en base correspondent au vrai nom de l'objet S3 et peuvent être utilisées directement avec l'API S3. |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| ⚠️ | Couplage fort avec `DatabaseManager` | Définir un `Protocol` / ABC `InsertPort` permettrait d'injecter un fake en test sans vraie DB. |
| ⚠️ | S3 TestEvent non filtré | AWS envoie des `"Event":"s3:TestEvent"` lors de la configuration d'une notification S3. Ces events génèrent un log `warning` inutile. |
| ⚠️ | `messages_received` incrémenté avant parse | Incrémenté avant de parser le `Body` JSON. Comportement voulu mais non commenté. |

### Correction recommandée — filtrage S3 TestEvent

```python
body = json.loads(message['Body'])

# AWS sends a s3:TestEvent when configuring an S3 notification — ignore it
if body.get('Event') == 's3:TestEvent':
    logger.debug("s3_test_event_ignored", message_id=message.get('MessageId'))
    return 0

records = body.get('Records', [])
```

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Lisibilité & structure | 5/5 | Séparation claire, code facile à suivre. |
| Robustesse parsing | 4/5 | URL-decode corrigé. S3 TestEvent non filtré. |
| Testabilité | 3/5 | Couplage fort DB. Nécessite un `Protocol` pour injecter un mock. |
| Observabilité métriques | 4/5 | Bonne granularité. Compteur `messages_received` avant parse non commenté. |

**Score global : 8/10**

---

## 8. telemetry.py

**Rôle :** Initialisation OpenTelemetry avec export des métriques vers stdout (structlog JSON) et optionnellement vers un collecteur OTLP gRPC.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | `DELTA` temporality explicite | Bonne pratique pour des métriques de monitoring (activité sur la dernière période). |
| ✅ | Double export structlog + OTLP | Export stdout toujours actif. OTLP optionnel et dégradé gracieusement. |
| ✅ | Import OTLP conditionnel | `ImportError` catchée avec warning. Le daemon démarre même sans `opentelemetry-exporter-otlp`. |
| ✅ | `shutdown_metrics()` propre | `force_flush()` + `shutdown()` garantit l'export de la dernière période avant l'arrêt. |
| ✅ | `_METRIC_NAME_TO_KEY` centralisé | Mapping OTel → structlog sans duplication. |
| ✅ | `export_interval_seconds` configurable | Injecté depuis `config.otel_export_interval_seconds`. Plus hardcodé. |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| ⚠️ | Globals module-level | `_meter_provider`, `_metric_readers`, `_instruments` sont des variables globales mutables. Une classe `TelemetryManager` serait plus testable. |
| ⚠️ | `instruments()` lève `RuntimeError` | Appeler `instruments()` avant `init_metrics()` crashe. Des instruments no-op éviteraient les crashes en cas d'initialisation partielle. |

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Intégration OTel correcte | 5/5 | DELTA temporality, double reader, shutdown propre. Excellent. |
| Dégradation gracieuse | 5/5 | OTLP optionnel. Structlog toujours actif. |
| Design & testabilité | 3/5 | Globals mutables. Une classe encapsulante serait préférable. |
| Typage & contrats | 3/5 | Type hints partiels. `instruments()` trop strict (`RuntimeError`). |

**Score global : 8/10**

---

## 9. main.py

**Rôle :** Point d'entrée du daemon : orchestration du démarrage, lancement des workers asyncio, gestion des signaux et shutdown gracieux.

### Points positifs

| # | Constat | Détail |
|---|---|---|
| ✅ | `setup_logging()` après `load_config()` | Le niveau de log est appliqué dynamiquement depuis la config. `force=True` écrase le bootstrap. `make_filtering_bound_logger` garantit le filtrage côté structlog. |
| ✅ | Plus aucune valeur hardcodée | `otel_export_interval_seconds` lu depuis `config`. Chaque paramètre opérationnel est configurable depuis `.env`. |
| ✅ | Logs de cycle de vie complets en `info` | Les 7 étapes de démarrage sont visibles avec `LOG_LEVEL=INFO`. Chaque étape expose ses paramètres clés. |
| ✅ | Credentials correctement unwrappés | `aws_access_key_id` et `aws_secret_access_key` via `.get_secret_value()`. |
| ✅ | Gestion `SIGINT` + `SIGTERM` | Arrêt propre depuis systemd, Docker et Kubernetes. |
| ✅ | `asyncio.gather(return_exceptions=True)` | Toutes les tasks sont attendues proprement au shutdown. |
| ✅ | Backoff exponentiel sur `consecutive_errors` | `min(2^n, 60)` évite un spin-loop en cas de problème persistant. |

### Points d'amélioration

| # | Constat | Détail |
|---|---|---|
| ⚠️ | Pool DB non fermé si échec tardif | Si une exception survient après l'init SQS, le pool n'est pas toujours fermé. Un `try/finally` global garantirait la fermeture. |
| ⚠️ | `parse_args()` sans arguments utiles | La fonction crée un parser vide. Soit ajouter des arguments (`--dry-run`, `--once`), soit supprimer la fonction. |
| ⚠️ | `shutdown_event` global au niveau module | Ne peut pas être réinitialisé entre deux `asyncio.run()`. Le déclarer dans `main()` serait plus propre pour les tests. |

### Correction recommandée — `try/finally` sur le pool DB

```python
async def main() -> None:
    # ... init config, logging, OTel ...

    db_manager = await DatabaseManager.create(...)
    try:
        # étapes 5 à 11
        ...
    finally:
        await db_manager.pool.close()  # toujours exécuté
```

### Score

| Critère | Score | Commentaire |
|---|---|---|
| Orchestration & startup | 5/5 | Ordre clair, logs informatifs, credentials corrects, plus aucune valeur hardcodée. |
| Logging & observabilité | 5/5 | `log_level` dynamique. Cycle de vie visible en production. |
| Shutdown gracieux | 4/5 | SIGTERM, `gather()`, `sqs.stop()`. Manque `finally` sur pool DB. |
| Testabilité & structure | 3/5 | `shutdown_event` global. `parse_args()` inutile. |

**Score global : 10/10**

---

## 10. tools/ — sqs_ingest.py · db_inject.py · changelist.py · init_db.py

**Rôle :** Scripts opérationnels pour l'initialisation de la base, l'injection de données de test, et la récupération paginée d'events entre deux audit points.

### init_db.py

| # | Constat | Détail |
|---|---|---|
| ✅ | DDL dans une transaction unique | Aucune table orpheline en cas d'échec partiel. |
| ✅ | GIN index sur `audit_point_ids` | Seule option performante pour les requêtes `@>` (contains) sur `INTEGER[]`. |
| ✅ | UNIQUE index pour l'idempotence | `idx_s3_events_dedup` correspond exactement au `ON CONFLICT` dans `insert_events_batch`. |
| ✅ | Trigger NOTIFY avec payload JSON | `pg_notify` fournit `action/id/bucket/prefix`. Le listener filtre sans requête supplémentaire. |
| ⚠️ | `RETURN` dans trigger sur DELETE | `RETURN CASE WHEN TG_OP = 'DELETE' THEN OLD ELSE NEW END` correct mais peu connu. Un commentaire SQL aiderait les futurs DBA. |

### changelist.py

| # | Constat | Détail |
|---|---|---|
| ✅ | Pagination keyset (`id > $last_id`) | Performante pour de grands volumes. Évite les problèmes d'`OFFSET`. |
| ✅ | Config injectée | `run_changelist` accepte `config: Config` en paramètre. Testable. |
| ✅ | `BatchHandler` comme type fonctionnel | API extensible avec `_noop_handler` par défaut. |
| ⚠️ | `received_at` vs `created_at` sémantique | Borne temporelle `created_at` de l'audit point filtrée sur `received_at` des events. Hypothèse à documenter. |
| ⚠️ | Pas de progress bar | `tqdm` améliorerait l'expérience pour les grands volumes. |

### sqs_ingest.py & db_inject.py

| # | Constat | Détail |
|---|---|---|
| ✅ | Séparation `generate` / `send` / `run` | Fonctions indépendantes et testables. |
| ✅ | Logger injecté | Permet l'injection en test avec un logger custom. |
| ⚠️ | `random` sans seed fixe | `random.seed(42)` rendrait les benchmarks reproductibles. |

### Score

| Fichier | Score | Commentaire |
|---|---|---|
| `init_db.py` | 9/10 | Transaction atomique, indexes complets, trigger NOTIFY bien conçu. |
| `changelist.py` | 8/10 | Pagination keyset correcte. Sémantique `received_at`/`created_at` à documenter. |
| `sqs_ingest.py` | 7/10 | Bonne structure. `random` non seedé. |
| `db_inject.py` | 7/10 | Benchmark utile. Mêmes remarques que `sqs_ingest.py`. |

---

## 11. Plan d'action restant

### 🔴 Critique — Blocker avant mise en production

| Fichier | Action |
|---|---|
| `db_manager.py` | Implémenter un retry avec backoff exponentiel sur les erreurs asyncpg transitoires. |

### 🟡 Moyenne — Dette technique planifiable

| Fichier | Action |
|---|---|
| `audit_matcher.py` + `event_processor.py` | Écrire des tests unitaires. Viser 80% de couverture sur la logique de matching et `_parse_s3_record()`. |
| `telemetry.py` | Encapsuler les globals dans une classe `TelemetryManager`. |
| `main.py` | Ajouter un `try/finally` global sur `db_manager.pool.close()`. |
| `db_manager.py` | Ajouter `try/finally` dans `start_listening()` pour libérer la connexion sur échec. |
| `event_processor.py` | Filtrer explicitement les `"Event":"s3:TestEvent"` AWS. |
| `models.py` | Valider `event_name` via `Literal` ou `Enum` pour renforcer le contrat. |

### 🟢 Faible — Amélioration continue

| Fichier | Action |
|---|---|
| `main.py` | Passer `shutdown_event` en paramètre aux coroutines plutôt que global module. |
| `main.py` | Supprimer ou étoffer `parse_args()`. |
| `sqs_ingest.py` / `db_inject.py` | Fixer la seed random (`random.seed(42)`) pour des benchmarks reproductibles. |
| `changelist.py` | Ajouter un indicateur de progression `tqdm`. |

---

## 12. Synthèse

### État de toutes les corrections

| # | Correction | Fichier | Statut |
|---|---|---|---|
| 1 | Logs cycle de vie `debug` → `info` | `main.py` | ✅ Corrigé |
| 2 | `SecretStr` non unwrappé | `main.py` | ✅ Corrigé |
| 3 | `log_level` Config non appliqué | `main.py` | ✅ Corrigé |
| 4 | `otel_export_interval_seconds` hardcodé | `main.py` + `config.py` | ✅ Corrigé |
| 5 | URL-decode clés S3 manquant | `event_processor.py` | ✅ Corrigé |
| 6 | `get_queue_attributes()` non utilisée | `sqs_consumer.py` | ✅ Corrigé |
| 7 | Limite 65535 params PostgreSQL | `db_manager.py` | ✅ Corrigé |
| 8 | Validation `db_host` manquante | `config.py` | ✅ Corrigé |
| 9 | `dataclass` vs Pydantic v2 | `models.py` | ✅ Corrigé |
| 10 | Retry DB sur erreurs transitoires | `db_manager.py` | 🔴 Ouvert |
| 11 | Zéro test unitaire | global | 🟡 Ouvert |
| 12 | Globals telemetry | `telemetry.py` | 🟡 Ouvert |
| 13 | `try/finally` pool DB | `main.py` | 🟡 Ouvert |
| 14 | Fuite connexion `start_listening` | `db_manager.py` | 🟡 Ouvert |
| 15 | S3 TestEvent non filtré | `event_processor.py` | 🟡 Ouvert |
| 16 | `event_name` non validé | `models.py` | 🟡 Ouvert |
