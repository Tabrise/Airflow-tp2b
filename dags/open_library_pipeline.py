"""
DAG : open_library_pipeline
Pipeline complet : Open Library API → transformation → PostgreSQL + traçabilité
"""

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.state import TaskInstanceState
from airflow.utils.trigger_rule import TriggerRule

# ─── Paramétrage centralisé ──────────────────────────────────────────────────
# Modifiez ces valeurs pour changer le comportement du pipeline
# sans toucher au code des tâches.
PIPELINE_CONFIG = {
    "keyword": "python",       # mot-clé de recherche Open Library
    "limit": 50,               # nombre maximum de résultats
    "source_name": "open_library",
    "base_url": "https://openlibrary.org/search.json",
    "postgres_conn_id": "postgres_default",
}

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS books (
    id                  SERIAL PRIMARY KEY,
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
    id               SERIAL PRIMARY KEY,
    source_name      TEXT        NOT NULL,
    search_keyword   TEXT        NOT NULL,
    started_at       TIMESTAMP,
    ended_at         TIMESTAMP,
    status           TEXT        NOT NULL,
    records_fetched  INTEGER     DEFAULT 0,
    records_loaded   INTEGER     DEFAULT 0
);
"""


# ─── Tâche 1 : Récupération des données depuis l'API ─────────────────────────

def fetch_books_from_api(**context):
    """Interroge l'API Open Library et pousse la réponse brute en XCom."""
    params = context["params"]
    keyword = params.get("keyword", PIPELINE_CONFIG["keyword"])
    limit   = params.get("limit",   PIPELINE_CONFIG["limit"])
    base_url = params.get("base_url", PIPELINE_CONFIG["base_url"])

    url = f"{base_url}?q={keyword}&limit={limit}"
    print(f"Appel API : {url}")

    response = requests.get(url, timeout=30)
    response.raise_for_status()

    books_raw = response.json().get("docs", [])

    ti = context["ti"]
    ti.xcom_push(key="books_raw",       value=books_raw)
    ti.xcom_push(key="records_fetched", value=len(books_raw))
    ti.xcom_push(key="started_at",      value=datetime.utcnow().isoformat())

    print(f"{len(books_raw)} livres récupérés pour le mot-clé « {keyword} »")


# ─── Tâche 2 : Transformation des données ────────────────────────────────────

def _quality_score(record: dict) -> int:
    """Calcule un score de complétude sur 7 champs informatifs."""
    fields = ["author_name", "isbn", "publisher", "language",
              "subject", "first_publish_year", "edition_count"]
    return sum(1 for f in fields if record.get(f))


def transform_books_data(**context):
    """
    Passe de la réponse JSON brute à des enregistrements structurés :
    - sélectionne les champs utiles
    - gère les champs absents (None par défaut)
    - simplifie les listes (premier élément retenu)
    - calcule un score de qualité de la donnée
    - conserve l'information sur la requête
    Les livres dont la transformation échoue sont ignorés sans faire planter le pipeline.
    """
    ti = context["ti"]
    params = context["params"]
    keyword     = params.get("keyword",     PIPELINE_CONFIG["keyword"])
    source_name = params.get("source_name", PIPELINE_CONFIG["source_name"])

    books_raw = ti.xcom_pull(key="books_raw", task_ids="fetch_books_from_api")
    loaded_at = datetime.utcnow().isoformat()

    transformed = []
    for raw in books_raw:
        try:
            def first(lst):
                """Retourne le premier élément d'une liste ou None."""
                items = raw.get(lst, [])
                return items[0] if items else None

            record = {
                "ol_key":             raw.get("key"),
                "title":              raw.get("title"),
                "author_name":        first("author_name"),
                "first_publish_year": raw.get("first_publish_year"),
                "isbn":               first("isbn"),
                "publisher":          first("publisher"),
                "language":           first("language"),
                "subject":            first("subject"),
                "has_full_text":      bool(raw.get("has_fulltext", False)),
                "edition_count":      raw.get("edition_count", 0) or 0,
                "search_keyword":     keyword,
                "source_name":        source_name,
                "loaded_at":          loaded_at,
            }
            record["data_quality_score"] = _quality_score(record)
            transformed.append(record)

        except Exception as exc:
            print(f"[WARN] Livre ignoré suite à une erreur : {exc}")
            continue

    ti.xcom_push(key="books_transformed", value=transformed)
    print(f"{len(transformed)} livres transformés (sur {len(books_raw)} bruts)")


# ─── Tâche 3 : Chargement dans PostgreSQL ────────────────────────────────────

def load_books_to_postgres(**context):
    """Insère les enregistrements transformés dans la table books."""
    ti    = context["ti"]
    books = ti.xcom_pull(key="books_transformed", task_ids="transform_books_data")

    hook   = PostgresHook(postgres_conn_id=PIPELINE_CONFIG["postgres_conn_id"])
    conn   = hook.get_conn()
    cursor = conn.cursor()

    insert_sql = """
        INSERT INTO books (
            ol_key, title, author_name, first_publish_year,
            isbn, publisher, language, subject,
            has_full_text, edition_count,
            search_keyword, source_name, data_quality_score, loaded_at
        ) VALUES (
            %(ol_key)s, %(title)s, %(author_name)s, %(first_publish_year)s,
            %(isbn)s, %(publisher)s, %(language)s, %(subject)s,
            %(has_full_text)s, %(edition_count)s,
            %(search_keyword)s, %(source_name)s, %(data_quality_score)s, %(loaded_at)s
        )
        ON CONFLICT (ol_key, search_keyword) DO NOTHING;
    """

    loaded_count = 0
    for book in books:
        try:
            cursor.execute(insert_sql, book)
            loaded_count += 1
        except Exception as exc:
            print(f"[WARN] Erreur insertion « {book.get('title')} » : {exc}")
            conn.rollback()

    conn.commit()
    cursor.close()
    conn.close()

    ti.xcom_push(key="records_loaded", value=loaded_count)
    print(f"{loaded_count} livres chargés dans PostgreSQL")


# ─── Tâche 4 : Traçabilité de l'exécution ────────────────────────────────────

def log_pipeline_execution(**context):
    """
    Écrit un enregistrement dans pipeline_executions.
    S'exécute même si une tâche précédente a échoué (trigger_rule=all_done),
    ce qui permet de tracer aussi les exécutions en erreur.
    """
    ti      = context["ti"]
    dag_run = context["dag_run"]
    params  = context["params"]

    # Détermine le statut global en vérifiant l'état des tâches critiques
    critical_tasks = ["fetch_books_from_api", "transform_books_data", "load_books_to_postgres"]
    all_success = all(
        dag_run.get_task_instance(task_id).state == TaskInstanceState.SUCCESS
        for task_id in critical_tasks
    )
    status = "success" if all_success else "failed"

    records_fetched = ti.xcom_pull(key="records_fetched", task_ids="fetch_books_from_api") or 0
    records_loaded  = ti.xcom_pull(key="records_loaded",  task_ids="load_books_to_postgres") or 0
    started_at      = ti.xcom_pull(key="started_at",      task_ids="fetch_books_from_api")

    keyword     = params.get("keyword",     PIPELINE_CONFIG["keyword"])
    source_name = params.get("source_name", PIPELINE_CONFIG["source_name"])

    hook   = PostgresHook(postgres_conn_id=PIPELINE_CONFIG["postgres_conn_id"])
    conn   = hook.get_conn()
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO pipeline_executions
            (source_name, search_keyword, started_at, ended_at, status, records_fetched, records_loaded)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
    """, (
        source_name,
        keyword,
        started_at,
        datetime.utcnow().isoformat(),
        status,
        records_fetched,
        records_loaded,
    ))

    conn.commit()
    cursor.close()
    conn.close()

    print(f"Exécution tracée — statut : {status} | récupérés : {records_fetched} | chargés : {records_loaded}")


# ─── Définition du DAG ───────────────────────────────────────────────────────

default_args = {
    "owner":           "mediathèque",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries":         1,
    "retry_delay":     timedelta(minutes=5),
}

with DAG(
    dag_id="open_library_pipeline",
    default_args=default_args,
    description="Pipeline Open Library → transformation → PostgreSQL avec traçabilité",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    params={
        "keyword":     PIPELINE_CONFIG["keyword"],
        "limit":       PIPELINE_CONFIG["limit"],
        "source_name": PIPELINE_CONFIG["source_name"],
        "base_url":    PIPELINE_CONFIG["base_url"],
    },
    tags=["mediathèque", "open_library", "ingestion"],
) as dag:

    create_tables = PostgresOperator(
        task_id="create_tables_if_not_exist",
        postgres_conn_id=PIPELINE_CONFIG["postgres_conn_id"],
        sql=CREATE_TABLES_SQL,
    )

    fetch_books = PythonOperator(
        task_id="fetch_books_from_api",
        python_callable=fetch_books_from_api,
    )

    transform_books = PythonOperator(
        task_id="transform_books_data",
        python_callable=transform_books_data,
    )

    load_books = PythonOperator(
        task_id="load_books_to_postgres",
        python_callable=load_books_to_postgres,
    )

    log_execution = PythonOperator(
        task_id="log_pipeline_execution",
        python_callable=log_pipeline_execution,
        trigger_rule=TriggerRule.ALL_DONE,  # s'exécute même en cas d'échec amont
    )

    # ─── Ordre des tâches ────────────────────────────────────────────────────
    create_tables >> fetch_books >> transform_books >> load_books >> log_execution
