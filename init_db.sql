-- Création de la base de données pour les données du TP
-- (la base "airflow" est créée automatiquement par Docker pour les métadonnées)
CREATE DATABASE mediatheque;

-- Connexion à la base mediatheque pour créer les tables
\connect mediatheque;

CREATE TABLE IF NOT EXISTS books (
    id                  SERIAL      PRIMARY KEY,
    ol_key              TEXT        NOT NULL,
    title               TEXT,
    author_name         TEXT,
    first_publish_year  INTEGER,
    isbn                TEXT,
    publisher           TEXT,
    language            TEXT,
    subject             TEXT,
    has_full_text       BOOLEAN     DEFAULT FALSE,
    edition_count       INTEGER     DEFAULT 0,
    search_keyword      TEXT        NOT NULL,
    source_name         TEXT        NOT NULL,
    data_quality_score  INTEGER     DEFAULT 0,
    loaded_at           TIMESTAMP   NOT NULL,
    CONSTRAINT books_ol_key_keyword_unique UNIQUE (ol_key, search_keyword)
);

CREATE TABLE IF NOT EXISTS pipeline_executions (
    id               SERIAL      PRIMARY KEY,
    source_name      TEXT        NOT NULL,
    search_keyword   TEXT        NOT NULL,
    started_at       TIMESTAMP,
    ended_at         TIMESTAMP,
    status           TEXT        NOT NULL,
    records_fetched  INTEGER     DEFAULT 0,
    records_loaded   INTEGER     DEFAULT 0
);
