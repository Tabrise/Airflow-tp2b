-- =============================================================================
-- Script de création des tables — TP 2B Open Library Pipeline
-- =============================================================================

-- Table principale : livres récupérés depuis Open Library
CREATE TABLE IF NOT EXISTS books (
    id                  SERIAL      PRIMARY KEY,

    -- Identifiant Open Library (ex : /works/OL82563W)
    ol_key              TEXT        NOT NULL,

    -- Informations bibliographiques
    title               TEXT,
    author_name         TEXT,                   -- premier auteur de la liste
    first_publish_year  INTEGER,
    isbn                TEXT,                   -- premier ISBN disponible
    publisher           TEXT,                   -- premier éditeur disponible
    language            TEXT,                   -- première langue disponible
    subject             TEXT,                   -- premier sujet/thème disponible
    has_full_text       BOOLEAN     DEFAULT FALSE,
    edition_count       INTEGER     DEFAULT 0,

    -- Contexte de l'ingestion
    search_keyword      TEXT        NOT NULL,   -- mot-clé utilisé pour la recherche
    source_name         TEXT        NOT NULL,   -- nom de la source (open_library)
    data_quality_score  INTEGER     DEFAULT 0,  -- 0-7 : nombre de champs renseignés
    loaded_at           TIMESTAMP   NOT NULL,

    -- Contrainte d'unicité : un même livre ne peut être chargé deux fois
    -- pour le même mot-clé (permet les re-runs idempotents)
    CONSTRAINT books_ol_key_keyword_unique UNIQUE (ol_key, search_keyword)
);

-- Index utiles pour les requêtes de contrôle
CREATE INDEX IF NOT EXISTS idx_books_keyword       ON books (search_keyword);
CREATE INDEX IF NOT EXISTS idx_books_author        ON books (author_name);
CREATE INDEX IF NOT EXISTS idx_books_publish_year  ON books (first_publish_year);
CREATE INDEX IF NOT EXISTS idx_books_quality_score ON books (data_quality_score);

-- Table de traçabilité : une ligne par exécution du pipeline
CREATE TABLE IF NOT EXISTS pipeline_executions (
    id               SERIAL      PRIMARY KEY,
    source_name      TEXT        NOT NULL,      -- source interrogée
    search_keyword   TEXT        NOT NULL,      -- paramètre de recherche utilisé
    started_at       TIMESTAMP,                 -- début de l'appel API
    ended_at         TIMESTAMP,                 -- fin de l'écriture en base
    status           TEXT        NOT NULL,      -- 'success' ou 'failed'
    records_fetched  INTEGER     DEFAULT 0,     -- enregistrements reçus de l'API
    records_loaded   INTEGER     DEFAULT 0      -- enregistrements insérés en base
);
