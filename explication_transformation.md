# Explication des choix de transformation — TP 2B

## Problématique

L'API Open Library renvoie un JSON dont les champs sont très hétérogènes selon les livres :
certains champs sont absents, d'autres sont des listes de longueur variable.
La transformation a pour but de produire des enregistrements **plats, complets et cohérents**
pour une table PostgreSQL.

---

## Choix de transformation

### 1. Simplification des listes → premier élément

Plusieurs champs de l'API (`author_name`, `isbn`, `publisher`, `language`, `subject`) sont des listes.
On retient le **premier élément** de chaque liste pour conserver une valeur par colonne.

**Justification :** la table cible est relationnelle (1 ligne = 1 livre).
Stocker des listes dans une colonne TEXT poserait des problèmes de requêtage.
Le premier auteur / premier ISBN est suffisant pour les besoins de la médiathèque.

### 2. Champs absents → NULL

Si un champ est absent de la réponse API, la valeur est `None` → `NULL` en base.
Aucune valeur par défaut arbitraire n'est inventée pour ne pas polluer les données.

**Justification :** un `NULL` est plus honnête qu'une valeur fictive comme `"Inconnu"`.
Les requêtes de contrôle peuvent ensuite cibler explicitement les `NULL`.

### 3. Score de qualité calculé

On calcule un `data_quality_score` de 0 à 7 comptant le nombre de champs
informatifs renseignés parmi : `author_name`, `isbn`, `publisher`, `language`,
`subject`, `first_publish_year`, `edition_count`.

**Justification :** permet d'identifier rapidement les enregistrements pauvres
en information sans avoir à tester chaque colonne séparément.

### 4. Conservation du contexte de requête

Chaque enregistrement conserve `search_keyword` et `source_name`.

**Justification :** le même livre peut apparaître pour des mots-clés différents.
La contrainte UNIQUE sur `(ol_key, search_keyword)` rend les re-runs idempotents
tout en permettant de stocker le même livre sous plusieurs thèmes.

### 5. Isolation des erreurs

La boucle de transformation englobe chaque livre dans un `try/except`.
Un livre malformé est ignoré avec un warning, sans faire échouer tout le pipeline.

**Justification :** l'API est publique et les données sont hétérogènes par nature.
La robustesse du pipeline prime sur l'exhaustivité d'un seul run.

---

## Structure des tables

| Table | Rôle |
|---|---|
| `books` | Stocke les livres transformés, 1 ligne par livre par mot-clé |
| `pipeline_executions` | Trace chaque exécution du pipeline (statut, compteurs, durée) |

---

## Paramétrage

Tous les paramètres modifiables sont centralisés dans `PIPELINE_CONFIG` en tête du DAG.
Ils peuvent être surchargés à l'exécution via l'interface Airflow (bouton *Trigger DAG w/ config*)
ou via la CLI :

```bash
airflow dags trigger open_library_pipeline \
  --conf '{"keyword": "history", "limit": 100}'
```
