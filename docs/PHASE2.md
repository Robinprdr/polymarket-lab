# Phase 2 — observation du complément YES/NO

READ ONLY — NO TRADING CAPABILITY. `EXECUTABLE` décrit uniquement une observation mathématique sous contraintes connues. Deux jambes CLOB ne sont pas atomiques : chaque résultat porte `MULTI_LEG_NON_ATOMIC`. Aucun ordre, fill, signature ou profit pratique garanti n'est produit.

## Lancement

Depuis le dossier de ce projet, avec `.venv` activé :

```sh
python -m polymarket_lab.monitor --scan-complements --max-markets 25 --duration 600 --database data/complement-10min.sqlite3
```

Cela observe jusqu'à 25 marchés découverts automatiquement (50 tokens s'ils sont tous binaires). Les 600 secondes incluent la découverte initiale. `Ctrl+C` interrompt proprement. Aucun changement des endpoints publics ou des dépendances. Sans `--scan-complements`, le runner conserve son fonctionnement d'observateur Phase 1.

## Architecture

- `complement.py` : validation du couple et optimisation pure, sans I/O.
- `opportunities.py` : épisodes et résumé de session ; aucune fonction réseau.
- `feed.py` : notification après application complète d'un événement, ou invalidation.
- `monitor.py` : scan des seuls marchés touchés ; contrôle de péremption des épisodes ouverts toutes les 100 ms.
- `storage.py` : migration additive SQLite version 2, épisodes et observations financières en texte.

## Calcul et optimisation

Pour chaque côté, trier tous les asks par prix croissant. Si les quantités des niveaux sont `s_i`, les prix `p_i` et `S_i` leur cumul :

```text
C(q) = Σ_i p_i × min(s_i, max(0, q − S_(i−1)))
GrossCost(q) = C_yes(q) + C_no(q)
GrossPayoff(q) = q
GrossEdge(q) = q − GrossCost(q)
GrossROI(q) = GrossEdge(q) / GrossCost(q)
```

Le domaine est borné par la profondeur totale la plus faible et par le maximum des tailles minimales connues des deux carnets. Le moteur compare l'union des cumuls de profondeur et des bornes du domaine. Sur chaque intervalle, le profit est affine : son maximum est atteint à une borne. Il n'y a donc ni grille approchée ni seuil de rentabilité. À profit égal, la plus petite quantité est retenue. Les coûts cumulés sont lus par recherche binaire ; le moteur restitue aussi les niveaux et tailles réellement consommés au q retenu.

Tout edge strictement positif au q optimal faisable est conservé. Si aucun q satisfaisant les minima de quantité connus n'a un edge positif, aucun épisode n'est ouvert. Les candidats intermédiaires ne deviennent pas des épisodes distincts.

Les coûts, quantités et edges utilisent un contexte Decimal dimensionné d'après les chiffres et exposants d'entrée afin de ne pas perdre un petit edge. Les divisions donnant des développements décimaux infinis (ROI, prix moyen) sont arrondies à 50 chiffres significatifs, uniquement en Decimal. Un coût total nul donne un ROI `null` (dénominateur nul), jamais une erreur ni un float infini.

## Validité et minima

Le scanner utilise `Market.yes_no` et exige exactement deux tokens distincts, le même marché et la même condition, des labels cohérents, un marché actif/non résolu et deux carnets `valid`, `ws_ready`, non périmés, avec asks et timestamps source. Le runner bloque en outre le scanner si la connexion n'est pas active ou si un restart est demandé. L'invalidation ferme immédiatement l'épisode, même sans message de prix suivant. Un snapshot résiduel d'une ancienne connexion ne peut rouvrir un épisode pendant un restart demandé.

Les `minimum_order_size` des carnets sont traités comme bornes sur q selon le contrat demandé pour le moteur de quantité. Les valeurs manquantes sont conservées `null` ; elles maintiennent le résultat `THEORETICAL`.

Attention aux unités : la [documentation Gamma actuelle](https://docs.polymarket.com/market-data/market-details) décrit `orderMinSize` comme un minimum en USDC. Ce champ brut, quand présent, est donc aussi vérifié contre le coût de chaque jambe ; il n'est pas utilisé pour fabriquer un minimum de contrats manquant. Si le q optimal brut ne satisfait pas cette vérification supplémentaire, il reste enregistré `THEORETICAL`, avec `minimum_notional_satisfied=false`. Cette version n'optimise pas séparément un second q sous contrainte notionnelle. La normalisation Phase 1 ne conserve pas la provenance/unité individuelle des minima : cette ambiguïté doit être levée avant d'utiliser ces mesures pour une exécution future.

## Frais et classification

La [documentation des métadonnées](https://docs.polymarket.com/market-data/market-details) définit `feesEnabled` comme l'activation des frais du marché. Seul le booléen explicite `false` donne ici `NOT_APPLICABLE`, `estimated_fees=0`, `net_edge=gross_edge`, `net_roi=gross_roi`. Ce zéro est fondé sur les métadonnées, jamais appliqué par défaut ni déduit d'une catégorie.

Tout autre cas donne `UNKNOWN`, avec `estimated_fees`, `net_edge`, `net_roi` à `null`, même si des paramètres partiels de frais existent. `KNOWN` est réservé à une future implémentation vérifiée du modèle applicable ; cette version n'en fabrique pas. La [documentation des frais](https://docs.polymarket.com/trading/fees) décrit un calcul dépendant du prix et des arrondis : le découpage effectif des fills et les règles applicables ne sont pas validés ici. Aucune catégorie ni taux n'est codé en dur.

`EXECUTABLE` exige le couple frais/non applicables démontré, les minima de quantité connus et respectés, la vérification notionnelle si fournie, des carnets frais/valides et un edge positif. Sinon un résultat brut positif reste `THEORETICAL`. Cette classification ne garantit pas les fills, le settlement ou la simultanéité des deux lectures publiques.

## Épisodes et données conservées

Identité d'un épisode : marché + condition + stratégie `STRICT_ARBITRAGE`. Un changement de q, de frais ou de statut d'observation met à jour le même épisode tant qu'un edge brut positif valide reste observé. Une disparition ferme l'épisode ; une réapparition en ouvre un nouveau UUID.

`first_seen_at` et `last_seen_at` sont les heures UTC des première et dernière observations positives distinctes. `duration_ms` est leur écart sur horloge monotone : une **borne observée**, pas une preuve de présence entre deux messages. Les répétitions sans nouvelle révision/métadonnée ne gonflent pas `observation_count`. `closed_at` marque la détection de fermeture ; une coupure, un arrêt ou une péremption est marquée comme observation censurée (`censored=true`). Après un arrêt brutal, les épisodes ouverts en base sont fermés `PROCESS_INTERRUPTED` au prochain démarrage du scanner, sans prolonger leur dernière observation.

`best_gross_edge_seen` conserve le meilleur profit brut ; `best_quantity_seen` est le q associé à ce meilleur profit (plus petit q initial en cas d'égalité). `best_roi_seen` et `best_net_edge_seen` sont des maxima indépendants. Les tables de recherche sont persistantes, sans filtrage par taille de profit : surveiller leur volume lors de longues sessions.

| Table | Contenu |
|---|---|
| `complement_opportunities` | Une ligne par épisode, champs temporels et financiers usuels en colonnes ; tous les champs demandés et les niveaux consommés dans `record_json` |
| `complement_observations` | Chaque observation positive après changement de révision/métadonnée ; payload intégral, horodatage et temps écoulé |

Les nombres financiers sont des chaînes décimales dans SQL et JSON, les compteurs/dates des entiers, les âges des mesures temporelles Phase 1. Exemple de requête pour des champs du payload :

```sql
SELECT market_id, gross_edge, optimal_quantity,
       json_extract(record_json, '$.yes_cost') AS yes_cost,
       json_extract(record_json, '$.yes_levels_consumed') AS yes_levels
FROM complement_opportunities;
```

Survie 50/100/250/500 ms : stockage préparé avec horizons, échantillons horodatés et bornes de durée, mais `survival_status=NOT_MEASURED`. Aucun booléen de survie n'est extrapolé à travers les gaps. La mesure explicite à ces horizons est reportée en Phase 2B.

## Console et résumé

Exemple synthétique exact (100 YES à 0.45, 100 NO à 0.50, frais inconnus) :

```text
COMPLEMENT OPEN market=7 q=100 gross_cost=95.00 gross_edge=5.00 gross_roi=5.2632% net_edge=UNKNOWN status=THEORETICAL age_yes_ms=0 age_no_ms=0 execution_risk=MULTI_LEG_NON_ATOMIC
```

Ouvertures, changements de statut et fermetures sont affichés. Les nouveaux meilleurs edges sont affichés au plus une fois par seconde par épisode ; cette limitation d'affichage ne filtre aucune observation SQLite. Les updates identiques ne spamment pas le terminal.

En fin de run, `COMPLEMENT SUMMARY` donne les marchés uniques examinés, les marchés à mapping binaire valide, le nombre d'épisodes, les épisodes encore ouverts (0 après arrêt propre), les maxima de profit/ROI/quantité, la médiane des durées observées et le nombre d'épisodes vus avec frais connus/non applicables ou inconnus. Un épisode changeant de statut de frais peut figurer dans les deux compteurs. Les durées censurées sont incluses dans cette statistique descriptive ; elle n'est pas un estimateur de survie.

## Validation et périmètre

Validation locale du 14 septembre 2026 : **92 tests réussis en 3,72 s** (`python -m pytest -q`), compilation Python et `pip check` réussis. Le lancement CLI de contrôle de 3 secondes est sorti avec le code 2, sans snapshot WebSocket, après des erreurs de transport Gamma. Il confirme le démarrage/arrêt du scanner et le résumé de session, pas une observation live réussie.

Fichiers créés : `polymarket_lab/complement.py`, `polymarket_lab/opportunities.py`, `tests/test_complement.py`, `tests/test_opportunities.py`, `tests/test_complement_integration.py`, `docs/PHASE2.md`.

Fichiers modifiés : `polymarket_lab/feed.py`, `polymarket_lab/monitor.py`, `polymarket_lab/storage.py`, `tests/test_read_only.py`, `README.md`.

Tests déterministes : cas sans arbitrage, single-level, profondeurs différentes, épuisement, portion profitable, niveaux défavorables, égalités, minima, très petits edges, rejet des flottants et valeurs invalides, frais inconnus, mapping et identité, stale/reconnect, épisodes, reprise après interruption et conservation SQLite. Une vérification indépendante en fractions exactes contrôle les optima de 50 paires de carnets synthétiques.

Le test end-to-end combine Gamma/REST simulés et un vrai serveur WebSocket local : deux snapshots, apparition après changement de prix, mise à jour de q dans le même épisode, disparition après un second changement de prix, puis contrôle des chiffres SQL et des messages sortants. Aucun GET par opportunité ni ordre envoyé. Les tests Phase 1 et garde-fous read-only restent exécutés.

Pas de validation live Phase 2 revendiquée par ces tests locaux. Les frais actifs restent inconnus, les minima manquent de provenance, les lectures publiques ne sont pas atomiques, les durées sont des bornes observées. Aucun NegRisk, shadow trading, dashboard ou moteur de Phase 3.
