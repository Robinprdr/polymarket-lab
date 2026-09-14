# Polymarket Lab — Arbitrage Engine

**Phase 2 : Complement Arbitrage Scanner. READ ONLY — NO TRADING CAPABILITY.**

Ce projet observe les métadonnées et les carnets Polymarket. L'option `--scan-complements` mesure les edges structurels YES/NO dans toute la profondeur disponible. Aucun portefeuille, signature, secret, SDK de trading ou endpoint d'envoi d'ordre. Voir [la documentation Phase 2](docs/PHASE2.md) pour le calcul exact, les frais, les épisodes et leurs limites.

## Installation

Python **3.12 ou plus récent** et Git sont nécessaires. Dans un terminal, ouvrez ce dossier, puis :

```sh
python3.12 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-lock.txt
python -m pytest -q
```

Le fichier `requirements-lock.txt` fixe aussi les dépendances indirectes et les outils de test, pour reproduire l'environnement validé. Pour une installation minimale : `python -m pip install -r requirements.txt`. Les tests s'installent via `requirements-dev.txt`.

Sur cette machine, Python 3.12 a été trouvé à cet emplacement ; le Python système est seulement en version 3.9 :

```sh
/Users/robinperdreau/.cache/codex-runtimes/codex-primary-runtime/dependencies/python/bin/python3 -m venv .venv
```

Un environnement `.venv` est déjà préparé localement. Il est exclu de Git et devra être recréé si le projet change de machine ou d'emplacement.

## Lancer et arrêter

Depuis le dossier du projet, après activation de `.venv` :

```sh
python -m polymarket_lab.monitor
```

Le programme parcourt toutes les pages du registre Gamma, conserve les marchés normalisés et suit au maximum **25 marchés** disposant de métadonnées actives, d'un carnet activé et du statut `acceptingOrders`. Ce statut est lu uniquement pour sélectionner des carnets pertinents. La sélection est déterministe par identifiant ; elle n'est pas fondée sur une rentabilité. Les identifiants sont découverts par API, sans liste codée en dur.

Un test court avec un échantillon explicitement incomplet :

```sh
python -m polymarket_lab.monitor --duration 120 --discovery-pages 1 --max-markets 5
```

Pour observer davantage de marchés :

```sh
python -m polymarket_lab.monitor --max-markets 100 --database data/observation.sqlite3
```

`Ctrl+C` ou `SIGTERM` arrête l'observation, ferme les connexions, invalide les carnets et écrit un dernier état de santé. `--duration` inclut le temps de démarrage. Un lancement sans aucun snapshot WebSocket se termine avec le **code 2**, même si le runner a correctement enregistré les erreurs. Ce code est un indicateur minimal ; un code 0 ne certifie pas une couverture exhaustive.

Le terminal affiche les marchés découverts et suivis, les tokens frais, le statut WebSocket, le nombre de messages, l'âge du dernier message, la couverture, la réconciliation, la taille SQLite et les erreurs. La taille inclut les fichiers SQLite WAL/SHM.

Options principales :

| Option | Défaut | Rôle |
|---|---:|---|
| `--max-markets` | 25 | Maximum suivi simultanément, entre 1 et 100 |
| `--discovery-pages` | 0 | 0 = parcours complet ; une limite positive signale `capped` |
| `--discovery-seconds` | 300 | Attente entre deux découvertes |
| `--reconcile-seconds` | 60 | Attente entre deux tournées REST |
| `--health-seconds` | 10 | Période des états de santé |
| `--snapshot-seconds` | 60 | Période des snapshots légers et de la purge |
| `--stale-seconds` | 30 | Seuil conservateur depuis la dernière mise à jour de profondeur |
| `--duration` | 0 | Durée maximale ; 0 = jusqu'à arrêt manuel |

## Architecture

```text
Gamma GET /markets/keyset → registre normalisé → markets / tokens (SQLite)
                                     ↓ sélection explicite
CLOB GET /book → bootstrap REST → carnets complets en mémoire
                                     ↑
WebSocket public market → snapshot initial → changements par niveau
                                     ↓
              santé / snapshots / incidents → SQLite + journal + terminal
CLOB GET /book périodique → comparaison prudente → incident + reconstruction si divergence
```

| Module | Responsabilité |
|---|---|
| `models.py` | Marchés, tokens, mapping YES/NO, Decimal, profondeur et âge |
| `public_api.py` | Deux routes HTTP GET autorisées, TLS vérifié, retries limités |
| `feed.py` | Abonnement public, heartbeat, événements atomiques, reconnexion |
| `monitor.py` | Découverte paginée, sélection, bootstrap, réconciliation et CLI |
| `storage.py` | SQLite, rétention, incidents et états de santé |

Les métadonnées conservent event/market/condition IDs, question, slug, outcomes et token IDs, statut, échéance, NegRisk, contraintes disponibles, frais, catégorie, règles et date de récupération. Les métadonnées brutes sont également enregistrées : les valeurs absentes restent inconnues, sans supposer des frais nuls. Un `event_id` unique est exposé seulement si l'association est non ambiguë ; toutes les associations brutes restent disponibles.

Le mapping YES/NO utilise les **libellés fournis avec les IDs**. Un tableau `[No, Yes]` est correctement interprété. Des libellés autres que YES/NO restent leurs véritables outcomes. Des longueurs différentes, des labels dupliqués ou des tokens invalides provoquent un rejet visible, jamais une déduction silencieuse.

Les prix et quantités sont des `Decimal` décodés directement depuis JSON. Les floats binaires sont rejetés par le modèle. SQLite les conserve en texte. Les niveaux sont triés bids décroissants / asks croissants à la lecture, quelle que soit l'ordre de l'API. Les deltas remplacent la quantité agrégée du niveau ; une taille zéro supprime le niveau. Toute la profondeur reçue reste en mémoire. Le module `complement.py` construit les coûts cumulés nécessaires au scanner optionnel.

## WebSocket, fraîcheur et couverture

L'abonnement utilise le canal public `market`, `assets_ids`, `initial_dump=true` et `custom_feature_enabled=true`. Aucune authentification n'est envoyée. Les événements `book`, `price_change`, `tick_size_change` et `market_resolved` sont gérés. Les événements d'information connus sans profondeur ne rafraîchissent pas les carnets. Un schéma inconnu ou invalide déclenche un incident et une reconstruction conservatrice.

Un `PING` texte est envoyé toutes les 10 secondes. Le flux est déclaré interrompu après 30 secondes sans message. L'absence de snapshots initiaux pendant 20 secondes déclenche aussi une reconnexion. Les tentatives utilisent un délai exponentiel plafonné avec aléa. Le compteur `reconnections` compte les nouvelles tentatives, y compris après un premier échec de connexion.

Le REST de démarrage permet d'inspecter et de conserver un premier carnet, mais **n'autorise pas l'application des deltas WebSocket**. Chaque session attend son propre snapshot initial. Les carnets deviennent invalides immédiatement à la déconnexion ou en cas d'erreur de parsing ; les deltas reçus sans baseline ne sont pas appliqués. Les changements touchant plusieurs tokens sont validés avant toute mutation.

Chaque carnet expose :

- `last_exchange_update` / `last_update` : timestamp source, s'il existe ;
- `last_local_update` / `local_receive_time` : heure UTC de réception ;
- `book_age_ms` : âge source constaté à réception, puis augmenté du temps monotone écoulé ; sans timestamp source REST, temps depuis réception uniquement ;
- `exchange_age_ms` : écart avec le timestamp source, dans les métriques de santé ;
- `valid`, `ws_ready`, `invalid_reason`, `stale` : qualité et disponibilité explicites.

L'âge source peut refléter un dernier changement ancien pour un carnet calme. Le modèle retient conservativement cet âge : recevoir maintenant un snapshot portant un timestamp ancien ne le rend pas frais. Un PONG ou un changement de tick ne remet pas l'âge du carnet à zéro. Un carnet calme devient donc conservativement périmé après 30 secondes, même si la connexion vit encore. Le temps écoulé utilise une horloge monotone ; l'âge source initial dépend néanmoins de la synchronisation de l'horloge UTC de la machine. Un timestamp de plus de cinq secondes dans le futur est rejeté et journalisé. Un consommateur futur doit vérifier fraîcheur ET validité, jamais seulement lire `bids` et `asks`.

`coverage_complete=false` si la découverte est partielle, ancienne, plafonnée par les abonnements, si un token suivi est périmé/invalide, ou si le WebSocket est déconnecté. La couverture porte uniquement sur les marchés jugés suivables par les métadonnées, pas sur tous les événements Polymarket. Le nombre de pertes est **inconnu** (`null`), car le protocole ne fournit pas de numéro de séquence global permettant de le prouver. Les messages invalides, événements inconnus, erreurs et interruptions sont comptés séparément.

Le registre est redécouvert périodiquement. Toute modification des abonnements provoque une nouvelle session et une période d'initialisation visible. Un registre complet devenu vide retire tous les abonnements. Les marchés absents d'un parcours complet restent archivés dans SQLite avec `in_latest_discovery=0` ; ils ne sont plus suivis. Un parcours partiel ne supprime pas silencieusement les anciennes métadonnées, mais ne sélectionne que les marchés validés pendant ce parcours.

## Réconciliation REST/WebSocket

Chaque tournée compare la **profondeur complète**, par token :

1. Capturer l'objet et sa révision avant le GET REST.
2. Valider le snapshot REST indépendamment du carnet courant.
3. Si un événement ou un changement d'abonnement a modifié le carnet pendant la requête, enregistrer `inconclusive_concurrent_update` et ne rien remplacer.
4. Si le timestamp REST est antérieur, enregistrer `inconclusive_older_rest`. Des timestamps manquants rendent la comparaison indéterminée.
5. Si les niveaux sont identiques, enregistrer `matched`. Ce constat ne rafraîchit pas artificiellement l'âge WebSocket.
6. Si les niveaux diffèrent sans course locale détectée, conserver les deux snapshots complets, journaliser la divergence, incrémenter les erreurs, invalider les carnets et demander une nouvelle session avec snapshots initiaux.

La correction ne consiste jamais à copier aveuglément un REST dans un carnet qui évolue. Un REST peut précéder un message WebSocket encore en transit ; une divergence signifie donc une **divergence observée**, pas une preuve de perte. Le choix conservateur peut provoquer des reconnexions inutiles. Les résultats par token et leur date restent dans `system_health`; les réinitialisations restent dans `incidents`. Une ancienne réussite de réconciliation n'est pas une garantie actuelle.

## Données et stockage

Par défaut, la base est `data/polymarket.sqlite3`, relative au répertoire depuis lequel vous lancez le programme. Le journal rotatif est `data/monitor.log` (2 Mo × 4 fichiers maximum). SQLite utilise WAL et un seul processus écrivain est prévu par base.

| Table | Contenu | Rétention |
|---|---|---|
| `markets` | Dernières métadonnées normalisées et brutes par marché | Persistante |
| `tokens` | Association token / marché / outcome | Persistante |
| `system_health` | Couverture, fraîcheur par token, compteurs, réconciliation | 7 jours |
| `book_snapshots` | Top 5 de chaque côté, explicitement marqué partiel, toutes les 60 s | 7 jours |
| `book_snapshots` complets | Bootstrap et comparaison autour des divergences | 24 heures |
| `incidents` | Découvertes, erreurs, connexions, déconnexions, divergences | 30 jours |

Les snapshots périodiques conservent les marqueurs d'invalidité et l'âge ; ils ne prétendent pas être frais. Les deltas ne sont pas tous archivés. Cette V1 permet de reproduire les tests et d'inspecter les états enregistrés, **pas de rejouer exactement tout le flux historique**. La taille dépend du nombre de tokens et de la fréquence des incidents. La purge réutilise l'espace SQLite ; elle ne réduit pas nécessairement la taille physique du fichier. Les métadonnées persistantes continuent de croître. Pour une réduction physique, arrêter le runner avant un `VACUUM` manuel.

Pour lire les dernières métriques avec le client SQLite :

```sh
sqlite3 data/polymarket.sqlite3 'SELECT metrics_json FROM system_health ORDER BY id DESC LIMIT 1;'
```

Le scanner optionnel ajoute des tables de recherche persistantes `complement_opportunities` et `complement_observations`. Chaque échantillon positif conserve les niveaux consommés ; il ne duplique pas toute la profondeur inutilisée. Ces tables ne sont pas purgées par la politique Phase 1.

## APIs officielles vérifiées

Documentation consultée le **13 septembre 2026** :

- [Découverte des marchés](https://docs.polymarket.com/market-data/discover-markets) : `GET https://gamma-api.polymarket.com/markets/keyset`, `closed=false`, pagination `next_cursor` → `after_cursor`.
- [Détails des marchés](https://docs.polymarket.com/market-data/market-details) : métadonnées et identification des tokens/outcomes.
- [Carnets](https://docs.polymarket.com/market-data/prices-order-books) : `GET https://clob.polymarket.com/book?token_id=...` et profondeur complète.
- [Spécification WebSocket officielle](https://docs.polymarket.com/asyncapi.json) : `wss://ws-subscriptions-clob.polymarket.com/ws/market`, messages et heartbeat.

Aucun appel POST n'est présent, même pour les lectures batch qui pourraient utiliser cette méthode. Le client n'accepte que deux routes GET fixes, refuse les paramètres inattendus, suit zéro redirection et ne lit aucune configuration de compte. La bibliothèque réseau générale reste capable de transporter des données, mais aucune capacité de trading n'est exposée par le projet. Les tests protègent cette surface ; modifier le code pourrait évidemment changer ses capacités.

## Validation et limites actuelles

Voir [le rapport de validation](docs/VALIDATION.md) et [les résultats structurés](docs/validation-results.json).

Les tests synthétiques couvrent le parsing, les nombres exacts, les snapshots/deltas, la fraîcheur, SQLite, la pagination, les limites, les divergences et les courses. Un vrai serveur WebSocket **local** vérifie la reconnexion ; un test de bout en bout relie découverte simulée, REST simulé, WebSocket local, SQLite et santé. Ces tests n'attestent pas la disponibilité du service Polymarket.

Le test Polymarket de cette session a échoué au contrôle TLS : certificat ne correspondant pas à `gamma-api.polymarket.com`. Aucun marché ni carnet réel n'a été obtenu par le runner. Ne pas désactiver TLS pour contourner ce problème : vérifier DNS, filtrage réseau, VPN ou proxy de la machine, puis relancer le test court. La cause précise du certificat incompatible n'a pas été établie.

Autres limites : un seul flux WebSocket, maximum opérationnel choisi de 100 marchés, snapshots REST séquentiels en réconciliation, pas de garantie de cohérence atomique entre plusieurs marchés, pas de preuve d'absence de pertes, compteurs en mémoire réinitialisés à chaque exécution mais historique SQLite conservé. La pagination ne représente pas un instantané atomique de tout Gamma. Les champs de frais, catégories et résolution sont conservés s'ils sont fournis ; ils ne sont ni calculés ni vérifiés économiquement. Les tests live prolongés restent nécessaires avant toute conclusion sur une observation continue.

## Git et phases suivantes

Dépôt local indépendant, branche `feature/read-only-data-foundation`. Aucun autre dépôt n'est modifié et aucun merge automatique n'est prévu. Les fichiers `.env`, bases, journaux et environnements virtuels sont exclus de Git.

La création distante n'a pas été possible : le CLI `gh` est absent et le connecteur GitHub disponible n'expose pas de création de repository. Pour publier :

1. Sur [GitHub, créer un repository](https://github.com/new) nommé `polymarket-lab`, de préférence privé, **vide** (sans README, licence ou `.gitignore` générés).
2. Copier son URL HTTPS, puis exécuter dans ce dossier, en remplaçant `VOTRE_COMPTE` :

```sh
git remote add origin https://github.com/VOTRE_COMPTE/polymarket-lab.git
git push -u origin feature/read-only-data-foundation
```

Si `origin` existe déjà, vérifier `git remote -v` avant toute modification. Ne pas intégrer de token dans l'URL. Utiliser l'authentification Git habituelle. Ces commandes ne fusionnent rien dans `main`.

La validation live Phase 1 a depuis été confirmée par l'utilisateur sur 25 marchés / 50 tokens. Les rapports Phase 1 ci-dessus sont historiques et décrivent le blocage réseau rencontré à leur date. La branche Phase 2 est `feature/complement-arbitrage-scanner`, issue de `a3e91904784467eb7963020a99c7223375143ad0`. Le test live Phase 2 se lance avec `python -m polymarket_lab.monitor --scan-complements --max-markets 25 --duration 600 --database data/complement-10min.sqlite3`. NegRisk et Shadow Execution restent hors périmètre ; aucune fusion dans `main`.
