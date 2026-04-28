# TP 2B — Pipeline complet API → transformation → PostgreSQL

## Objectif

Pipeline orchestré avec Apache Airflow qui récupère des livres depuis l'API Open Library, les transforme et les charge dans PostgreSQL, avec traçabilité complète de chaque exécution.

---

## Structure du projet

```
tp2b/
├── dags/
│   └── open_library_pipeline.py   # DAG Airflow complet
├── sql/
│   ├── create_tables.sql          # Script de création des tables PostgreSQL
│   └── control_queries.sql        # Requêtes SQL de contrôle
├── explication_transformation.md  # Choix de transformation justifiés
└── README.md
```

---

## API utilisée

**Open Library Search API** — `https://openlibrary.org/search.json`

Exemple d'appel : `https://openlibrary.org/search.json?q=python&limit=50`

L'API retourne un JSON avec un tableau `docs` contenant les livres correspondant au mot-clé.

---

## Pipeline — étapes du DAG

Le DAG `open_library_pipeline` enchaîne 5 tâches :

```
create_tables_if_not_exist
          ↓
  fetch_books_from_api
          ↓
  transform_books_data
          ↓
  load_books_to_postgres
          ↓
  log_pipeline_execution
```

| Tâche | Rôle |
|---|---|
| `create_tables_if_not_exist` | Crée les tables `books` et `pipeline_executions` si elles n'existent pas encore |
| `fetch_books_from_api` | Appelle l'API Open Library et pousse la réponse brute en XCom |
| `transform_books_data` | Transforme le JSON brut en enregistrements structurés et compatibles PostgreSQL |
| `load_books_to_postgres` | Insère les enregistrements dans la table `books` |
| `log_pipeline_execution` | Écrit le bilan de l'exécution dans `pipeline_executions` (même en cas d'échec) |

---

## Paramétrage

Tous les paramètres sont centralisés dans `PIPELINE_CONFIG` en tête du DAG :

```python
PIPELINE_CONFIG = {
    "keyword":     "python",
    "limit":       50,
    "source_name": "open_library",
    "base_url":    "https://openlibrary.org/search.json",
    "postgres_conn_id": "postgres_default",
}
```

Ils peuvent être surchargés à chaque exécution sans modifier le code :

- **Via l'UI Airflow** : bouton *Trigger DAG w/ config*, puis saisir :
  ```json
  { "keyword": "history", "limit": 100 }
  ```

- **Via la CLI** :
  ```bash
  airflow dags trigger open_library_pipeline \
    --conf '{"keyword": "artificial intelligence", "limit": 75}'
  ```

---

## Tables PostgreSQL

### Table `books`

Stocke les livres transformés. Une ligne = un livre pour un mot-clé donné.

```sql
CREATE TABLE IF NOT EXISTS books (
    id                  SERIAL      PRIMARY KEY,
    ol_key              TEXT        NOT NULL,        -- clé Open Library (/works/...)
    title               TEXT,
    author_name         TEXT,                        -- premier auteur
    first_publish_year  INTEGER,
    isbn                TEXT,                        -- premier ISBN
    publisher           TEXT,                        -- premier éditeur
    language            TEXT,
    subject             TEXT,                        -- premier sujet/thème
    has_full_text       BOOLEAN     DEFAULT FALSE,
    edition_count       INTEGER     DEFAULT 0,
    search_keyword      TEXT        NOT NULL,        -- mot-clé de la recherche
    source_name         TEXT        NOT NULL,
    data_quality_score  INTEGER     DEFAULT 0,       -- 0-7 : champs renseignés
    loaded_at           TIMESTAMP   NOT NULL,
    CONSTRAINT books_ol_key_keyword_unique UNIQUE (ol_key, search_keyword)
);
```

### Table `pipeline_executions`

Trace chaque exécution du pipeline.

```sql
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id               SERIAL      PRIMARY KEY,
    source_name      TEXT        NOT NULL,
    search_keyword   TEXT        NOT NULL,
    started_at       TIMESTAMP,
    ended_at         TIMESTAMP,
    status           TEXT        NOT NULL,    -- 'success' ou 'failed'
    records_fetched  INTEGER     DEFAULT 0,
    records_loaded   INTEGER     DEFAULT 0
);
```

---

## Transformation des données

### Problème

L'API Open Library renvoie des champs hétérogènes : certains sont absents, d'autres sont des listes de longueur variable.

### Choix appliqués

| Problème | Solution retenue | Justification |
|---|---|---|
| Champs de type liste (`author_name`, `isbn`, `publisher`, `language`, `subject`) | Premier élément retenu | La table est relationnelle, 1 colonne = 1 valeur |
| Champs absents | `NULL` en base | Plus honnête qu'une valeur fictive, permet de cibler les données manquantes |
| Livre malformé | Ignoré avec warning, le pipeline continue | Robustesse prioritaire sur l'exhaustivité |
| Re-runs du pipeline | Contrainte `UNIQUE(ol_key, search_keyword)` + `ON CONFLICT DO NOTHING` | Idempotence : relancer le DAG ne duplique pas les données |
| Évaluation de la complétude | `data_quality_score` : compte les champs renseignés sur 7 | Permet de repérer les livres trop incomplets sans tester chaque colonne |

---

## Requêtes SQL de contrôle

Les requêtes complètes se trouvent dans [`sql/control_queries.sql`](sql/control_queries.sql).

**Livres chargés par mot-clé**
```sql
SELECT search_keyword, COUNT(*) AS nb_livres
FROM books
GROUP BY search_keyword
ORDER BY nb_livres DESC;
```

**Livres sans auteur**
```sql
SELECT ol_key, title, search_keyword
FROM books
WHERE author_name IS NULL;
```

**Livres avec peu d'informations (score ≤ 2)**
```sql
SELECT title, data_quality_score
FROM books
WHERE data_quality_score <= 2
ORDER BY data_quality_score;
```

**Top auteurs**
```sql
SELECT author_name, COUNT(*) AS nb_livres
FROM books
WHERE author_name IS NOT NULL
GROUP BY author_name
ORDER BY nb_livres DESC
LIMIT 20;
```

**Années de publication les plus représentées**
```sql
SELECT first_publish_year, COUNT(*) AS nb_livres
FROM books
WHERE first_publish_year IS NOT NULL
GROUP BY first_publish_year
ORDER BY nb_livres DESC
LIMIT 20;
```

**Historique des exécutions tracées**
```sql
SELECT
    search_keyword,
    started_at,
    ended_at,
    status,
    records_fetched,
    records_loaded
FROM pipeline_executions
ORDER BY started_at DESC;
```

---

## Installation et lancement

### Prérequis

- Apache Airflow 2.x avec le provider PostgreSQL installé :
  ```bash
  pip install apache-airflow-providers-postgres
  ```
- PostgreSQL accessible depuis Airflow
- Python `requests` disponible dans l'environnement Airflow

### Configuration

1. Copier `dags/open_library_pipeline.py` dans le dossier `dags/` d'Airflow.
2. Créer une connexion Airflow nommée `postgres_default` :
   - **Conn Type** : Postgres
   - **Host** : `localhost` (ou l'hôte de votre PostgreSQL)
   - **Schema** : nom de la base cible
   - **Login / Password** : identifiants PostgreSQL
   - **Port** : `5432`

### Lancement

Activer le DAG `open_library_pipeline` dans l'UI Airflow, puis cliquer **Trigger DAG**.

Les tables sont créées automatiquement à la première exécution.

---

## Choix de conception

- **Séparation stricte des responsabilités** : chaque tâche a un rôle unique (fetch / transform / load / log), conformément aux contraintes du TP.
- **Pas de JSON brut en base** : la réponse API n'est jamais insérée directement ; elle transite par XCom entre les tâches.
- **Traçabilité systématique** : la tâche `log_pipeline_execution` utilise `trigger_rule=ALL_DONE` pour s'exécuter même si une tâche précédente a échoué, afin de toujours enregistrer le bilan.
- **Valeurs codées en dur limitées** : seul le `postgres_conn_id` est fixé dans `PIPELINE_CONFIG` car il dépend de la configuration Airflow, pas du métier.
