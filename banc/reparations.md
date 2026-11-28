| Id | Cause attendue | Cause observée | Décision attendue | Décision observée | Règle attendue | Règle appliquée | Builds | OK |
|---|---|---|---|---|---|---|---|---|
| P01 | colonne_renommee | colonne_renommee | reparer | reparer | alias | alias | 2 | oui |
| P02 | colonne_renommee | colonne_renommee | reparer | reparer | alias | alias | 2 | oui |
| P03 | colonne_renommee | colonne_renommee | reparer | reparer | alias | alias | 2 | oui |
| P04 | type_modifie | type_modifie | reparer | reparer | cast | cast | 2 | oui |
| P05 | type_modifie | type_modifie | reparer | reparer | cast | cast | 2 | oui |
| P06 | verrou_transitoire | verrou_transitoire | reparer | reparer | retry | retry | 2 | oui |
| P07 | colonne_supprimee | colonne_supprimee | escalader | escalader | None | None | 1 | oui |
| P08 | lignes_perdues | lignes_perdues | escalader | escalader | None | None | 1 | oui |
| P09 | empreinte_incoherente | empreinte_incoherente | ne_pas_reparer | ne_pas_reparer | None | None | 1 | oui |
| P10 | valeurs_derivees | valeurs_derivees | escalader | escalader | None | None | 1 | oui |
