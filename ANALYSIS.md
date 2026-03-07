# Analyse du projet FSU — FastScan Unified (2026)

> Revue technique complète : points forts, points faibles, et recommandations.

---

## Points positifs ✅

### Architecture & Design

- **Séparation des responsabilités claire** : chaque module a un rôle bien délimité (`sqs_consumer`, `event_processor`, `audit_matcher`, `db_manager`, `telemetry`). Le code est lisible et maintenable.
- **Design asynchrone cohérent** : l'utilisation d'`asyncio` + `aioboto3` + `asyncpg` est pertinente et cohérente pour un service I/O-bound à fort débit. Pas de mélange sync/async problématique.
- **Pattern LISTEN/NOTIFY PostgreSQL** : élégant pour propager les changements d'audit points en temps réel sans polling ni redémarrage. C'est une vraie bonne idée d'architecture.
- **Hiérarchie de préfixes** : le matching multi-niveaux (`/data`, `/data/2024`, `/data/2024/dir1`) avec retour de tous les IDs correspondants est un design solide qui évite les duplications en base.
- **Idempotence** : l'index `UNIQUE (bucket, object_key, event_time, event_name)` avec `ON CONFLICT DO NOTHING` protège contre les rejeux SQS sans logique applicative complexe.
- **Graceful shutdown** : gestion correcte des signaux `SIGINT`/`SIGTERM`, annulation des tâches asyncio, flush des métriques avant fermeture.
- **Configuration centralisée** : usage de `pydantic-settings` sans singleton global, `load_config()` retourne directement une instance `Config()`. La racine du projet est détectée dynamiquement via `Path(__file__)`, ce qui évite les problèmes de CWD.
- **Injection de dépendance pour les instruments OTel** : `instruments` est passé en paramètre à `SQSConsumer` et `EventProcessor`, évitant toute dépendance globale implicite.

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

### Robustesse & Gestion d'erreurs

- **`insert_events_batch` : pas de retry** — si l'insert échoue (réseau, dépassement de pool), l'exception remonte mais le message SQS sera quand même supprimé si d'autres erreurs n'ont pas été détectées au niveau du `process_message`. Il faudrait distinguer les erreurs transientes (retry) des erreurs définitives (dead-letter queue).
- **Pas de Dead Letter Queue (DLQ)** mentionnée dans l'architecture : en production AWS SQS, une DLQ est essentielle pour capturer les messages qui ne peuvent pas être traités après N tentatives.
- **Pas de circuit breaker** sur les appels PostgreSQL ou SQS : si la base est saturée, le service peut accumuler des erreurs et le backoff exponentiel seul ne suffit pas toujours.
- **`consecutive_errors` se réinitialise à 0 dès qu'un batch réussit** : une erreur intermittente toutes les 5 opérations ne sera jamais détectée comme problème persistant.

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

| Dimension | Évaluation |
|---|---|
| Architecture | ⭐⭐⭐⭐⭐ Excellente |
| Performance | ⭐⭐⭐⭐ Bonne (COPY FROM non applicable avec ON CONFLICT) |
| Sécurité | ⭐⭐⭐ Correcte pour dev, insuffisante pour prod |
| Robustesse | ⭐⭐⭐⭐ Bonne, manque DLQ et retry fin |
| Observabilité | ⭐⭐⭐⭐ Bonne (OTLP optionnel + DELTA natif) |
| Tests | ⭐ Absent — point critique |
| Qualité code | ⭐⭐⭐⭐⭐ Excellente |
| Documentation | ⭐⭐⭐⭐⭐ Excellente |

**Conclusion** : FSU est un projet de très bonne facture avec une architecture bien pensée, particulièrement le pattern LISTEN/NOTIFY, le matching hiérarchique et l'instrumentation OTel. Les lacunes principales pour une mise en production sérieuse en 2026 sont l'absence totale de tests automatisés, la gestion des secrets, et la nécessité d'un endpoint de healthcheck exposable. Ces points sont corrigibles sans remettre en question l'architecture générale, qui reste excellente.

---

*Analyse rédigée en mars 2026.*
