# Rapport — Phase 1 Read-Only Data Foundation

**Implémentation locale livrée ; validation opérationnelle Polymarket encore bloquée.**

READ ONLY — NO TRADING CAPABILITY. Aucun scanner Complement, NegRisk, moteur de coût ou exécuteur n'a été ajouté.

## 1. Architecture créée

Dépôt Git indépendant `polymarket-lab`, Python 3.12, six modules : registre normalisé et carnets exacts, transport GET public à routes fermées, flux WebSocket public, SQLite, orchestration CLI. Le bootstrap REST ne sert pas de baseline aux deltas WebSocket : chaque connexion attend ses propres snapshots. Les états incomplets sont explicites.

## 2. Fichiers créés

```text
polymarket-lab/
├── README.md
├── .gitignore
├── pyproject.toml
├── requirements.txt
├── requirements-dev.txt
├── requirements-lock.txt
├── polymarket_lab/
│   ├── __init__.py
│   ├── models.py
│   ├── public_api.py
│   ├── feed.py
│   ├── storage.py
│   └── monitor.py
├── tests/
│   ├── conftest.py
│   ├── test_models.py
│   ├── test_storage_api.py
│   ├── test_monitor.py
│   ├── test_feed.py
│   ├── test_read_only.py
│   └── fixtures/
│       ├── market.json
│       └── book.json
└── docs/
    ├── VALIDATION.md
    ├── validation-results.json
    ├── network-check.json
    └── test-results.xml
```

L'environnement `.venv`, la base `data/polymarket.sqlite3` et le journal local existent également, mais sont exclus de Git. Aucun fichier du projet Market Lab equities n'a été utilisé.

## 3. APIs officielles utilisées

Documentation actuelle consultée le 13 septembre 2026 avant implémentation :

| Usage | API publique | Référence |
|---|---|---|
| Découverte | Gamma `GET /markets/keyset`, `closed=false`, `limit`, `after_cursor` | [Discover Markets](https://docs.polymarket.com/market-data/discover-markets) |
| Métadonnées | Réponse Gamma, outcomes et token IDs associés | [Market Details](https://docs.polymarket.com/market-data/market-details) |
| Profondeur REST | CLOB `GET /book?token_id=...` | [Prices and Order Books](https://docs.polymarket.com/market-data/prices-order-books) |
| Temps réel | `wss://ws-subscriptions-clob.polymarket.com/ws/market` | [Spécification AsyncAPI](https://docs.polymarket.com/asyncapi.json) |

Ces routes sont implémentées. Leur exploitation réelle dans cet environnement reste bloquée par TLS. Aucun SDK de trading ni secret n'est nécessaire.

## 4. Nombre réellement découvert lors du test

Test live borné exécuté le **13 septembre 2026**, démarré vers **08:36:29 UTC** :

```sh
python -m polymarket_lab.monitor --duration 15 --discovery-pages 1 --max-markets 2 --health-seconds 3
```

| Mesure réelle | Résultat |
|---|---:|
| Marchés découverts par le runner | **0** |
| Tokens découverts par le runner | **0** |
| Snapshots REST obtenus | **0** |
| Snapshots WebSocket Polymarket obtenus | **0** |
| Code de sortie | **2** |

Ces zéros décrivent l'échec d'accès, pas l'absence de marchés sur Polymarket. La découverte Gamma échoue après trois tentatives GET sur un certificat ne correspondant pas au hostname. L'erreur, les métriques et l'état de couverture incomplet ont bien été conservés dans SQLite.

Le test synthétique de bout en bout, distinct du test live, découvre **1 marché fictif et 2 tokens fictifs**, récupère deux snapshots REST simulés, reçoit deux snapshots sur un serveur WebSocket local, réconcilie et persiste les données. Il ne constitue pas une preuve de couverture du service réel.

## 5. Exemple réel de carnet récupéré

**Indisponible : aucun carnet réel n'a pu être récupéré.** Fournir un exemple réel serait inventer un résultat. La fixture `tests/fixtures/book.json` est explicitement synthétique ; elle sert notamment à vérifier la conservation exacte de `12.34567890123456789` contrats et le tri des niveaux. Un snapshot réel complet sera enregistré automatiquement après le premier bootstrap réussi.

## 6. Fonctionnement du WebSocket

Abonnement `market` par `assets_ids`, snapshots initiaux demandés, heartbeat texte PING/PONG, changements agrégés BUY/SELL appliqués par niveau, suppression à taille zéro. Les changements multi-token sont validés avant application. Reconnexion avec délai exponentiel et aléa, invalidation des carnets à la coupure, attente obligatoire d'une nouvelle baseline. Les changements de tick ne rajeunissent pas la profondeur ; une résolution invalide définitivement la condition pendant la session du programme.

**Validé localement** : un serveur WebSocket réel sur loopback coupe la première connexion ; le client se reconnecte, reçoit un nouveau snapshot et applique le delta suivant. Les tests vérifient aussi messages invalides, silence, absence de baseline et arrêt propre.

**Non validé sur Polymarket** : handshake, abonnement et stabilité prolongée. Un contrôle TLS indépendant à 08:41:18 UTC échoue sur Gamma, CLOB et le hostname WebSocket. Voir `network-check.json`.

## 7. Stratégie de stockage

SQLite WAL, un seul écrivain. Métadonnées persistantes ; santé et top 5 par côté sur 7 jours ; snapshots complets au bootstrap et aux divergences sur 24 heures ; incidents sur 30 jours ; journal rotatif borné. Toute la profondeur reste en mémoire. Les nombres exacts sont stockés sous forme de chaînes. Les snapshots partiels sont marqués comme tels. Aucun archivage illimité de tous les deltas, aucun replay historique complet promis.

## 8. Réconciliation REST/WebSocket

Comparaison de toute la profondeur, avec vérification de l'identité et de la révision locale capturée avant le GET. Si un update arrive pendant la requête, le résultat est indéterminé et aucun écrasement ne se produit. Un REST antérieur est également indéterminé. Une divergence conserve les deux carnets, incrémente les erreurs et demande une reconstruction WebSocket visible. Sans séquence commune REST/WS, une divergence observée peut aussi refléter des messages en transit : elle n'est pas présentée comme une perte prouvée.

## 9. Tests exécutés et résultats

Sur Python **3.12.14** :

```text
python -m pytest -q --junitxml=docs/test-results.xml
46 passed in 3.62s

python -m compileall -q polymarket_lab
OK

python -m pip check
No broken requirements found.

git diff --check
OK
```

Couverture fonctionnelle des tests : mapping YES/NO inversé et rejet des métadonnées ambiguës ; précision Decimal ; reconstruction et tri des carnets ; deltas absolus et suppression ; atomicité ; horloge monotone, source ancienne et timestamp futur ; reconnexion et nouvelle baseline ; parsing invalide et timeouts ; résolution ; SQLite et rétention ; GET exclusifs, paramètres filtrés et refus des redirections ; pagination et curseur répété ; registre vide ; couverture plafonnée ; réconciliation identique, divergente ou concurrente ; runner de bout en bout avec APIs simulées et WebSocket local.

Le test `test_read_only.py` garde la surface d'import et de méthodes réseau fermée. Il s'agit d'un garde-fou de régression, pas d'une preuve formelle de sécurité.

Les résultats machine et les métriques réellement persistées se trouvent dans `validation-results.json` ; le rapport de tests est `test-results.xml`.

## 10. Limitations et problèmes observés

- **Blocage réseau réel** : `SSLCertVerificationError`, hostname mismatch sur les trois services. La cause précise (DNS, filtrage, proxy, etc.) n'est pas établie ; aucun contrôle TLS n'a été désactivé.
- **Acceptation live incomplète** : découverte réelle, exemple réel de carnet, maintenance temps réel et reconnexion au service réel restent à vérifier.
- Couverture limitée par défaut à 25 marchés ; maximum opérationnel de 100. Cette sélection n'est pas exhaustive ni optimisée par activité.
- Pas de séquences globales permettant de prouver l'absence de pertes ; compteur des pertes inconnu. Pas de snapshot atomique multi-marchés.
- Fraîcheur conservatrice : un snapshot ancien côté source reste périmé même reçu récemment ; des marchés calmes peuvent donc rester signalés périmés. L'horloge UTC de la machine doit être correctement synchronisée.
- Réconciliation séquentielle ; comparaisons souvent indéterminées possibles sur des carnets très actifs. Reconstruction de toute la session en cas de divergence, avec une coupure de couverture visible.
- Métadonnées disponibles conservées, mais frais et règles non validés économiquement. Aucune logique de profit n'existe.
- SQLite réutilise l'espace purgé sans nécessairement réduire le fichier ; les métadonnées continuent de croître.
- **GitHub non créé/non poussé** : `gh` absent, connecteur sans outil de création de repository. Le dépôt local est prêt.

## 11. Git et commit

Branche : `feature/read-only-data-foundation`.

Commit d'implémentation et de tests :

```text
9a84848adbd26144f52bdfed5c3d70545f0174ea
Build read-only Polymarket registry, books, streaming and health foundation
```

Un second commit ajoute la documentation et les preuves de validation. Son hash final est communiqué dans la réponse de livraison et s'obtient avec `git rev-parse HEAD`. Aucune fusion dans `main`. L'identité d'auteur locale utilisée est `Codex <codex@localhost>` ; aucune identité Git globale n'a été modifiée.

Action exacte pour publier : créer sur [GitHub](https://github.com/new) un repository `polymarket-lab` vide, puis depuis ce dossier, avec votre nom de compte :

```sh
git remote add origin https://github.com/VOTRE_COMPTE/polymarket-lab.git
git push -u origin feature/read-only-data-foundation
```

## 12. Prochaines étapes recommandées

1. Résoudre le certificat incompatible / accès réseau aux trois hôtes publics ; conserver TLS vérifié.
2. Relancer le test court de 120 secondes décrit dans le README. Vérifier une découverte non nulle, des snapshots REST, des snapshots et deltas WebSocket, les frais disponibles et la correspondance outcome/token.
3. Effectuer une observation de plusieurs heures, puis prolongée : examiner les périodes de couverture incomplète, ancienneté, réconciliation, volume SQLite et reconnexions. Tester une coupure réseau contrôlée.
4. Valider les critères d'observation avant d'entamer Phase 2 Cost/Depth Engine. NegRisk et Shadow Execution restent ultérieurs.

À ce stade, la réponse à « observons-nous Polymarket correctement et continuellement ? » est : **le socle et ses comportements locaux sont vérifiés ; la preuve opérationnelle sur Polymarket manque encore à cause du blocage TLS.**
