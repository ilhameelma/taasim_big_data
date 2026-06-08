# TaaSim — Rapport ML Semaine 6
## Prédiction de la Demande de Taxis à Casablanca
**ENSA Al Hoceima — Capstone 2025-2026**
**Personne B — Training, Évaluation, Baseline, Sauvegarde**

---

## 1. Contexte et Objectif

Le projet TaaSim simule une plateforme de mobilité urbaine (Taxi-as-a-Service) pour la ville de Casablanca. L'objectif de la semaine 6 (Partie B) est de construire un modèle de Machine Learning capable de **prédire la demande de taxis** par zone et par créneau horaire de 30 minutes.

**Données source :** Trajets de taxis de Porto (Portugal, Jul 2013 – Jun 2014), remappés sur les 17 arrondissements de Casablanca par le pipeline de feature engineering (Personne A).

**Critère de réussite :** `RMSE_modèle < RMSE_baseline`

---

## 2. Données : Feature Matrix

La Feature Matrix est produite par `feat_engineering.py` (Personne A) et stockée dans MinIO :
```
s3a://machine-learning/features/feature_matrix/
```

| Dimension | Valeur |
|-----------|--------|
| Total lignes | 162,495 |
| Zones | 17 arrondissements (zone_id 0 → 16) |
| Période | Juillet 2013 → Juin 2014 (12 mois) |
| Granularité | 1 ligne = 1 zone × 1 slot de 30 min |

### 2.1 Features utilisées (14 au total)

| Catégorie | Feature | Type | Description |
|-----------|---------|------|-------------|
| **Temporelle** | `hour_of_day` | int | Heure 0–23 |
| **Temporelle** | `day_of_week` | int | Jour 1=Dim … 7=Sam |
| **Temporelle** | `is_weekend` | 0/1 | Samedi ou Dimanche |
| **Temporelle** | `is_friday` | 0/1 | Vendredi (contexte marocain) |
| **Spatiale** | `zone_population_density` | float | Densité population |
| **Spatiale** | `zone_type_urban` | 0/1 | One-hot zone urbaine |
| **Spatiale** | `zone_type_residential` | 0/1 | One-hot zone résidentielle |
| **Spatiale** | `zone_type_commercial` | 0/1 | One-hot zone commerciale |
| **Spatiale** | `zone_type_transit_hub` | 0/1 | One-hot hub de transit |
| **Météo** | `is_raining` | 0/1 | Pluie (Open-Meteo API) |
| **Météo** | `temp_bucket_idx` | float | Température encodée (cold/mild/hot) |
| **Lag** | `demand_lag_1d` | float | Demande J-1 même slot |
| **Lag** | `demand_lag_7d` | float | Demande J-7 même slot |
| **Lag** | `rolling_7d_mean` | float | Moyenne glissante 7 jours |

**Label (target) :** `demand` — nombre de taxis partis d'une zone dans un slot de 30 min.

---

## 3. Split Temporel (B2)

**Principe :** Split strict sans shuffle pour éviter le data leakage temporel.

```
Jul 2013 ──────────────────────── Avr 2014 | Mai 2014 ── Jun 2014
◄──────────────── TRAIN (10 mois) ─────────►◄── TEST (2 mois) ──►
         133,528 lignes (82.2%)                28,967 lignes (17.8%)
```

| Ensemble | Lignes | Période | Dernier slot |
|----------|--------|---------|--------------|
| **Train** | 133,528 | Jul 2013 → Avr 2014 | 2014-04-30 18:00 |
| **Test** | 28,967 | Mai 2014 → Jun 2014 | 2014-06-30 23:30 |

---

## 4. Pipeline ML (B3)

```
temperature_bucket (string)
        │
        ▼
   StringIndexer → temp_bucket_idx (float)
        │
        ▼ (+ 13 features numériques)
  VectorAssembler → features_raw (vecteur dense 14D)
        │
        ▼
  StandardScaler → features (μ=0, σ=1)
        │
        ▼
  GBTRegressor → prediction
```

### Paramètres d'entraînement (B4)

| Paramètre | Valeur |
|-----------|--------|
| Algorithme | Gradient Boosted Trees Regressor |
| `maxDepth` | 5 (initial) → 7 (best CV) |
| `maxIter` | 50 |
| `seed` | 42 |
| `featuresCol` | features |
| `labelCol` | demand |

---

## 5. Cross-Validation (B5)

**Configuration :** 3 folds, grille sur `maxDepth=[5, 7]`, métrique RMSE.

| maxDepth | RMSE moyen CV | Sélection |
|----------|--------------|-----------|
| 5 | 3.8209 | — |
| **7** | **3.7302** | ✅ Meilleur |

Le modèle avec `maxDepth=7` a été retenu et sauvegardé dans MinIO :
```
s3a://machine-learning/models/demand_v1/
```

---

## 6. Évaluation sur le Test Set (B6)

Le modèle GBT avec `maxDepth=7` évalué sur les 28,967 lignes du test set (Mai–Jun 2014) :

| Métrique | Valeur | Interprétation |
|----------|--------|----------------|
| **RMSE** | **5.0621** | Erreur moyenne ±5 taxis par slot |
| **MAE** | **2.8430** | Erreur absolue moyenne ±2.8 taxis |
| **R²** | **0.6267** | 62.7% de la variance expliquée |

---

## 7. Baseline Naïve (B7)

La baseline utilise `demand_lag_7d` comme prédiction : *"La demande de la semaine prochaine = la demande de cette semaine."*

| Métrique | Baseline (lag_7d) |
|----------|-------------------|
| RMSE | 7.6285 |
| MAE | 4.5861 |
| R² | 0.1529 |

---

## 8. Comparaison Modèle vs Baseline (B8)

| Métrique | GBT Modèle | Baseline (lag_7d) | Amélioration |
|----------|-----------|-------------------|--------------|
| **RMSE** | **5.0621** | 7.6285 | **+33.6%** ✅ |
| **MAE** | **2.8430** | 4.5861 | **+38.0%** ✅ |
| **R²** | **0.6267** | 0.1529 | +47.4 pts ✅ |

**✅ CRITÈRE CAPSTONE VALIDÉ : RMSE_modèle (5.06) < RMSE_baseline (7.63)**

---

## 9. Feature Importance (B9)

Importance calculée par GBTRegressor (réduction totale de la variance sur tous les arbres) :

| Rang | Feature | Importance | Interprétation |
|------|---------|-----------|----------------|
| 🥇 1 | `rolling_7d_mean` | **41.2%** | La moyenne hebdomadaire prédit bien la demande |
| 🥈 2 | `hour_of_day` | **16.0%** | La demande varie fortement selon l'heure |
| 🥉 3 | `demand_lag_7d` | **8.9%** | Le même slot il y a 7 jours est informatif |
| 4 | `demand_lag_1d` | 7.7% | Corrélation avec J-1 |
| 5 | `is_weekend` | 7.5% | Weekend vs semaine |
| 6 | `day_of_week` | 7.2% | Motifs hebdomadaires |
| 7 | `zone_population_density` | 5.1% | Densité de la zone |
| 8 | `temp_bucket_idx` | 2.7% | Température (faible impact) |
| 9 | `is_raining` | 2.5% | Pluie (faible impact) |
| 10 | `is_friday` | 1.2% | Vendredi spécifique |
| 11-14 | `zone_type_*` | 0.0% | Non discriminants |

**Observation :** Les features de lag temporel (`rolling_7d_mean`, `demand_lag_7d`, `demand_lag_1d`) représentent à elles seules **57.8%** de l'importance totale, confirmant que la demande de taxi suit des patterns hebdomadaires réguliers.

---

## 10. Comparaison par Zone (B12)

Performance du modèle sur les 17 zones de Casablanca :

| Zone | Slots test | Demande moy | RMSE | MAE | Difficulté |
|------|-----------|-------------|------|-----|------------|
| 0 | 805 | 1.37 | 0.69 | 0.50 | 🟢 Facile |
| 1 | 2092 | 10.63 | 7.07 | 4.37 | 🟡 Moyen |
| 2 | 2095 | 10.49 | 6.84 | 4.72 | 🟡 Moyen |
| **3** | **2182** | **25.00** | **12.37** | **8.91** | **🔴 Difficile** |
| 4 | 2005 | 6.00 | 3.31 | 2.40 | 🟢 Facile |
| 5 | 1958 | 5.15 | 2.79 | 2.09 | 🟢 Facile |
| 6 | 1736 | 2.95 | 1.66 | 1.27 | 🟢 Facile |
| 7 | 1972 | 4.63 | 2.65 | 2.01 | 🟢 Facile |
| 8 | 2114 | 12.96 | 6.66 | 4.74 | 🟡 Moyen |
| **12** | **40** | **1.05** | **0.63** | **0.57** | **🟢 Meilleure** |
| 13 | 1338 | 1.95 | 1.31 | 0.90 | 🟢 Facile |
| 14 | 1368 | 1.92 | 1.08 | 0.84 | 🟢 Facile |

**Observation :** La zone 3 (forte demande moyenne = 25 taxis/slot) est la plus difficile à prédire (RMSE=12.37). Les zones à faible demande (0, 12, 13, 14) sont très bien prédites. Le RMSE est corrélé avec le niveau de demande moyen de la zone.

---

## 11. Fichiers produits dans MinIO

| Chemin MinIO | Contenu | Tâche |
|-------------|---------|-------|
| `machine-learning/models/demand_v1/` | Modèle PipelineModel Spark | B10 |
| `machine-learning/metrics/evaluation_metrics.json` | RMSE, MAE, R², comparaison baseline | B11 |
| `machine-learning/metrics/feature_importance.json` | Top 14 features avec scores | B11 |
| `machine-learning/metrics/zone_comparison.json` | Métriques par zone (JSON) | B12 |
| `machine-learning/metrics/zone_comparison.csv` | Métriques par zone (CSV) | B12 |

---

## 12. Scripts Python produits

| Fichier | Tâches | Description |
|---------|--------|-------------|
| `train_model.py` | B1→B5 | Lecture, split, pipeline, entraînement, CV |
| `evaluate_model.py` | B6→B8 | Évaluation, baseline, comparaison |
| `feature_importance.py` | B9 | Extraction et affichage des importances |
| `save_model.py` | B10→B12 | Vérification modèle, upload métriques, tableau zones |

---

## 13. Conclusions

### Ce qui fonctionne bien
- Le modèle GBT bat la baseline de **33.6%** sur le RMSE → critère capstone validé.
- Les features de lag temporel sont les plus importantes (57.8% de l'importance totale).
- Les zones à faible demande sont très bien prédites (RMSE < 1.5).

### Limites identifiées
- La zone 3 (centre-ville, forte demande) reste difficile à prédire précisément.
- Les features `zone_type_*` ont une importance nulle → peuvent être retirées dans une version future.
- Le CrossValidator utilisé fait un k-fold classique (shuffle), non temporel → légère surestimation des performances CV.

### Pistes d'amélioration
- Ajouter des features d'événements spéciaux (Ramadan, fêtes nationales marocaines).
- Utiliser un TimeSeriesSplit pour la validation croisée.
- Tester Random Forest ou XGBoost pour comparer.
- Augmenter `maxIter` à 100 pour améliorer la zone 3.

---

*Rapport généré le 04 Juin 2026 — TaaSim Capstone ENSA Al Hoceima 2025-2026*
