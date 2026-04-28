-- =============================================================================
-- Requêtes SQL de contrôle — TP 2B Open Library Pipeline
-- =============================================================================


-- 1. Combien de livres ont été chargés (total et par mot-clé) ?
SELECT
    search_keyword,
    COUNT(*)           AS nb_livres,
    MIN(loaded_at)     AS premier_chargement,
    MAX(loaded_at)     AS dernier_chargement
FROM books
GROUP BY search_keyword
ORDER BY nb_livres DESC;


-- 2. Quels livres n'ont pas d'auteur renseigné ?
SELECT
    ol_key,
    title,
    search_keyword,
    loaded_at
FROM books
WHERE author_name IS NULL
ORDER BY title;


-- 3. Quels livres ont peu d'informations disponibles (score de qualité faible) ?
-- Un score ≤ 2 sur 7 indique un enregistrement très incomplet
SELECT
    ol_key,
    title,
    author_name,
    data_quality_score,
    search_keyword
FROM books
WHERE data_quality_score <= 2
ORDER BY data_quality_score ASC, title;


-- 4. Quels auteurs reviennent le plus souvent ?
SELECT
    author_name,
    COUNT(*) AS nb_livres
FROM books
WHERE author_name IS NOT NULL
GROUP BY author_name
ORDER BY nb_livres DESC
LIMIT 20;


-- 5. Quelles années de publication sont les plus représentées ?
SELECT
    first_publish_year,
    COUNT(*) AS nb_livres
FROM books
WHERE first_publish_year IS NOT NULL
  AND first_publish_year BETWEEN 1800 AND EXTRACT(YEAR FROM NOW())::INTEGER
GROUP BY first_publish_year
ORDER BY nb_livres DESC
LIMIT 20;


-- 6. Répartition des scores de qualité (vue d'ensemble de la qualité des données)
SELECT
    data_quality_score,
    COUNT(*)                                        AS nb_livres,
    ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 1) AS pourcentage
FROM books
GROUP BY data_quality_score
ORDER BY data_quality_score;


-- 7. Histogramme des langues représentées
SELECT
    COALESCE(language, '(non renseigné)') AS langue,
    COUNT(*) AS nb_livres
FROM books
GROUP BY language
ORDER BY nb_livres DESC;


-- 8. Livres avec texte intégral disponible
SELECT
    title,
    author_name,
    first_publish_year,
    search_keyword
FROM books
WHERE has_full_text = TRUE
ORDER BY first_publish_year DESC;


-- =============================================================================
-- Requêtes de contrôle sur la traçabilité
-- =============================================================================

-- 9. Historique complet des exécutions
SELECT
    id,
    source_name,
    search_keyword,
    started_at,
    ended_at,
    EXTRACT(EPOCH FROM (ended_at - started_at))::INTEGER AS duree_secondes,
    status,
    records_fetched,
    records_loaded,
    ROUND(100.0 * records_loaded / NULLIF(records_fetched, 0), 1) AS taux_chargement_pct
FROM pipeline_executions
ORDER BY started_at DESC;


-- 10. Résumé des exécutions par statut
SELECT
    status,
    COUNT(*)           AS nb_executions,
    SUM(records_fetched) AS total_recuperes,
    SUM(records_loaded)  AS total_charges
FROM pipeline_executions
GROUP BY status;
