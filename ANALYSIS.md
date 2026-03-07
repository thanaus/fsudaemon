# Analyse du projet FSU — FastScan Unified (2026)

> Revue technique complète : points forts, points faibles, et recommandations.

---

## Points positifs ✅

### Architecture & Design

- **Séparation des responsabilités claire** : chaque module a un rôle bien délimité (`sqs_consumer`, `event_processor`, `audit_matcher`, `db_manager`, `telemetry`). Le code est lisible et maintenable.
- **Design asynchrone cohérent** : l'utilisation d'`asyncio` + `aioboto3` + `asyncpg` est pertinente et cohérente pour un service I/O-bound à fort débit. Pas de mélange sync/async problématique.
- **Pattern LISTEN/NOTIFY PostgreSQL** : élégant pour propager les changements d'audit points en temps réel sans polling ni redémarrage. C'est une vraie bonne idée d'architecture.
- **Hiérarchie de préfixes** : le matching multi-niveaux (`/data`, `/data/2024`, `/data/2024/dir1`) avec retour de tous les IDs correspondants est un design solide qui évite les duplications en base.
- **Rechargement atomique des audit points** : dans `load_audit_points`, la nouvelle structure est entièrement construite avant d'être assignée en une seule opération à `self.audit_points`. Un consommateur concurrent ne verra jamais d'état intermédiaire vide lors d'un rechargement à chaud.
- **Normalisation des préfixes** : le suffixe `/` est ajouté automatiquement si absent, sauf pour le préfixe vide (qui signifie "tout le bucket"), ce qui garantit la cohérence du `startswith` dans le matching.
- **Validation explicite des données** : bucket vide et prefix `None` lèvent une `ValueError` avec un message incluant l'`id` fautif, ce qui facilite le diagnostic en production.
- **Idempotence** : l'index `UNIQUE (bucket, object_key, event_time, event_name)` avec `ON CONFLICT DO NOTHING` protège contre les rejeux SQS sans logique applicative complexe.
- **Graceful shutdown** : gestion correcte des signaux `SIGINT`/`SIGTERM`, annulation des tâches asyncio, flush des métriques avant fermeture.
- **Fiabilité de la file de messages** : en cas d'erreur lors du traitement, le message SQS n'est pas acquitté et réapparaît automatiquement dans la queue pour être retraité. Aucun événement ne peut être perdu silencieusement.
- **Configuration centralisée** : usage de `pydantic-settings` sans singleton global, `load_config()` retourne directement une instance `Config()`. La racine du projet est détectée dynamiquement via `Path(__file__)`, ce qui évite les problèmes de CWD.
- **`SecretStr` pour les données sensibles** : `db_password` et `aws_secret_access_key` sont typés `SecretStr`, ce qui évite leur affichage accidentel dans les logs ou les tracebacks (repr masqué automatiquement par pydantic).
- **Validators explicites sur tous les paramètres critiques** : plages valides et messages d'erreur clairs sur `sqs_batch_size`, `sqs_wait_time_seconds`, `sqs_visibility_timeout`, `log_level` et `otlp_endpoint`. Une mauvaise configuration est rejetée dès le démarrage.
- **`get_db_dsn()` encapsulé** : le DSN est construit via `get_secret_value()`, le mot de passe ne transite jamais comme `str` brut à l'extérieur de la classe.
- **Injection de dépendance pour les instruments OTel** : `instruments` est passé en paramètre à `SQSConsumer` et `EventProcessor`, évitant toute dépendance globale implicite.
- **Client SQS persistant** : le client HTTP est ouvert une seule fois au démarrage via `start()` et fermé proprement au shutdown via `stop()`. Pas de reconnexion TLS à chaque batch, ce qui réduit la latence et le CPU à fort débit.
- **Pattern factory async `DatabaseManager.create()`** : impossible d'appeler `__init__` directement avec une coroutine — ce pattern est la bonne pratique pour les classes nécessitant une initialisation asynchrone.
- **`max_inactive_connection_lifetime=300`** : les connexions inactives sont recyclées après 5 min, évitant les connexions zombies côté PostgreSQL. Paramètre de production souvent oublié.
- **`time.perf_counter()` ciblé sur `conn.execute`** : mesure le temps DB pur sans inclure l'acquisition de connexion depuis le pool — métrique précise et exploitable.

### Base de données

- **GIN index sur `audit_point_ids[]`** : choix correct pour les requêtes `@>` sur tableau PostgreSQL. Très performant à grande échelle.
- **Index B-tree ciblés** : `event_time DESC`, `received_at DESC`, `bucket+object_key`, `event_name` — couverture correcte des cas d'usage évidents.
- **Soft delete** sur `audit_points` (`deleted_at`) : permet de conserver l'historique et de ne pas casser les événements déjà enregistrés qui référencent un audit point supprimé.
- **Multi-row INSERT** : l'insertion par batch avec expansion de paramètres ($1, $2...) est bien plus performante que des INSERT individuels. `COPY FROM` serait encore plus rapide mais ne supporte pas nativement `ON CONFLICT` — le multi-row INSERT est donc le bon compromis.

### Outillage & Développement

- **`tools/sqs_ingest.py`** : outil de test réaliste, génère des événements avec variété de préfixes et types d'événements, envoie par batch de 10 (limite SQS respectée).
- **`tools/db_inject.py`** : benchmark direct DB sans passer par SQS, utile pour isoler les performances d'insertion.
- **`tools/changelist.py`** : pagination par keyset (`AND id > last_id`) plutôt qu'`OFFSET/LIMIT` — choix de performance important documenté et justifié.
- **LocalStack** : support natif via `AWS_ENDPOINT_URL`, permettant un développement 100% local sans compte AWS.
- **Logs structurés JSON** (structlog) : facilite l'intégration avec des outils d'observabilité (Loki, Datadog, Splunk…).
- **OpenTelemetry** : temporalité DELTA native via `AggregationTemporality.DELTA` sur le reader — le SDK calcule les deltas nativement. Export optionnel vers un collector OTLP (`OTLP_ENDPOINT`) en parallèle du stdout structlog, sans modifier le comportement existant.
- **`pyproject.toml`** : contrainte `python = ">=3.12"`, dépendances avec bornes hautes, dépendances dev séparées (`pytest-asyncio`, `ruff`, `mypy`).
- **README complet** : architecture diagrammée, exemples SQL, FAQ, guide de déploiement Docker, cas d'usage multiples — documentation de qualité professionnelle.

---

## Points négatifs / Axes d'amélioration ⚠️

### Sécurité

- **Credentials AWS en clair dans `.env`** : en 2026, la pratique recommandée est d'utiliser des IAM Roles (ECS Task Role, EC2 Instance Profile, IRSA pour Kubernetes) ou AWS Secrets Manager. Le README mentionne les IAM roles comme optionnel mais ne décourage pas l'usage des clés statiques.
- **Mot de passe PostgreSQL en clair dans `.env`** : aucune intégration avec un gestionnaire de secrets (AWS Secrets Manager, HashiCorp Vault, Doppler…). En production, c'est un risque si le fichier `.env` est mal protégé ou commité par erreur.
- **Pas de `.gitignore` visible** : le fichier `.env` (avec credentials) pourrait être commité accidentellement. Un `.gitignore` explicite avec `.env` est indispensable.
- **`docker-compose.yml` expose le port PostgreSQL** (`${DB_PORT}:5432`) sur toutes les interfaces par défaut. En production, il vaudrait mieux le restreindre à `127.0.0.1:${DB_PORT}:5432`.

### `db_manager.py` — Qualité du code

Aucun point négatif — les corrections suivantes ont été appliquées : imports `typing` obsolètes remplacés par les built-ins natifs et `collections.abc`, `on_change_callback` typé `Callable[[], Coroutine[Any, Any, None]]`, `self.conn` renommé `self._conn`, `_pending_tasks` paramétré en `set[asyncio.Task[None]]`, transaction explicite supprimée avec commentaire explicatif, et `tools/db_inject.py` + `tools/changelist.py` mis à jour pour utiliser `DatabaseManager.create()` directement à la place de l'import `create_db_pool` inexistant.

### `config.py` — Qualité du code

Aucun point négatif — les corrections suivantes ont été appliquées : `Optional` remplacé par `str | None` (Python 3.10+), `case_sensitive=True` pour cohérence avec Linux, `@model_validator` ajouté pour la validation croisée `db_pool_min_size`/`db_pool_max_size`, et `@lru_cache(maxsize=1)` sur `load_config()` pour garantir une instance unique.

### Robustesse & Gestion d'erreurs

- **Pas de retry sur `insert_events_batch`** : si l'insert échoue de manière transitoire (pic de connexions, timeout réseau), l'exception remonte correctement et le message SQS n'est pas supprimé (il réapparaîtra après le `VisibilityTimeout`). Cependant, l'absence de retry applicatif avec backoff exponentiel signifie que chaque erreur DB consomme inutilement un cycle de visibilité SQS complet (jusqu'à 30 secondes par défaut).
- **Pas de Dead Letter Queue (DLQ)** mentionnée dans l'architecture : en production AWS SQS, une DLQ est essentielle pour capturer les messages qui ne peuvent pas être traités après N tentatives, et éviter qu'ils bloquent la queue indéfiniment.
- **Pas de circuit breaker** sur les appels PostgreSQL ou SQS : si la base est saturée, le service peut accumuler des erreurs et le backoff exponentiel seul ne suffit pas toujours.
- **`consecutive_errors` se réinitialise à 0 dès qu'un batch réussit** : une erreur intermittente toutes les 5 opérations ne sera jamais détectée comme problème persistant. Ce mécanisme protège contre les pannes franches, mais pas contre la dégradation progressive.

### `AuditPointMatcher` — Qualité du code

- **Complexité O(n) dans `get_matching_audit_points`** : la boucle itère sur tous les préfixes du bucket à chaque événement. Négligeable avec peu d'audit points, mais dès quelques milliers de préfixes par bucket, une structure de type trie ou une liste triée avec `bisect` serait significativement plus performante. Ce point rejoint la limite mentionnée sur les >100k audit points en mémoire.

### Performance & Scalabilité

- **Expansion de paramètres dans `insert_events_batch`** : générer `$1, $2, ..., $N` pour un batch de 500 événements × 7 colonnes = 3500 paramètres. PostgreSQL a une limite de 65535 paramètres, mais surtout la construction de la requête en Python (concaténation de chaînes) devient coûteuse pour de très grands batches. `COPY FROM` (`conn.copy_records_to_table`) serait nettement plus performant à haut débit, mais ne supporte pas nativement `ON CONFLICT` — c'est pourquoi le multi-row INSERT a été conservé.
- **Un seul worker SQS** (`process_messages_loop`) : la parallélisation est limitée. Pour des débits très élevés, il faudrait plusieurs workers concurrents consommant la même queue (ou des shards séparés).
- **`AuditPointMatcher` en mémoire** : correct pour des milliers d'audit points, mais sans limite de taille ni mécanisme d'expiration. Un volume très important d'audit points (>100k) pourrait poser problème.

### Observabilité & Monitoring

- **Pas d'endpoint de healthcheck HTTP** : en 2026, un service de production expose typiquement `/healthz` ou `/readyz` pour les orchestrateurs (Kubernetes, ECS). Le healthcheck PostgreSQL existe en interne mais n'est pas exposable depuis l'extérieur.
- **Pas de tracing distribué** (OpenTelemetry traces) : les spans de traitement SQS → DB ne sont pas instrumentés, ce qui rend le debugging de latences difficile.
- **Pas d'alerte configurée** sur les métriques (taux d'erreur, lag SQS, etc.).

### Tests

- **Aucun test automatisé visible** (pas de `tests/`, pas de `pytest`, pas de `unittest`). En 2026 c'est un manque critique pour un service de production traitant des données d'audit.
- **Pas de tests unitaires pour `AuditPointMatcher`** : la logique de matching hiérarchique est au cœur du service et mérite une couverture exhaustive (préfixes vides, cas limites, buckets inexistants…).
- **Pas de tests d'intégration** avec LocalStack ou une base PostgreSQL de test.
- **`tools/db_inject.py` et `tools/sqs_ingest.py`** font office de tests de charge mais pas de validation fonctionnelle.

### Documentation

- **Pas de `CHANGELOG`** ni de versioning sémantique du projet.
- **Pas de documentation sur les limites de débit** attendues (events/sec, messages/sec) ni sur le dimensionnement recommandé du pool PostgreSQL selon la charge.

---

## Résumé

| Dimension | Évaluation | Notes |
|---|---|---|
| Architecture | ⭐⭐⭐⭐⭐ | Excellente |
| Performance | ⭐⭐⭐⭐ | Bonne (COPY FROM non applicable avec ON CONFLICT) |
| Sécurité | ⭐⭐⭐ | Correcte pour dev, insuffisante pour prod |
| Robustesse | ⭐⭐⭐⭐ | Bonne, manque DLQ et retry fin sur les erreurs DB transitoires |
| Observabilité | ⭐⭐⭐⭐ | Bonne (OTLP optionnel + DELTA natif), manque healthcheck HTTP |
| Tests | ⭐ | Absent — point critique |
| Qualité code | ⭐⭐⭐⭐⭐ | Excellente |
| Documentation | ⭐⭐⭐⭐⭐ | Excellente |

**Conclusion** : FSU est un projet de très bonne facture avec une architecture bien pensée, particulièrement le pattern LISTEN/NOTIFY, le matching hiérarchique et l'instrumentation OTel. Les lacunes principales pour une mise en production sérieuse en 2026 sont : l'absence totale de tests automatisés, la gestion des secrets, et l'absence d'un endpoint de healthcheck exposable. Ces points sont tous corrigibles sans remettre en question l'architecture générale, qui reste excellente.

---

*Analyse révisée en mars 2026.*
