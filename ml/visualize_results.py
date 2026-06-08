"""
visualize_results.py
TaaSim — Capstone Project · ENSA Al Hoceima · 2025-2026
Week 6 — Visualisation des résultats ML

Graphiques produits :
  1. Comparaison RMSE / MAE / R² : Modèle vs Baseline
  2. Feature Importance (top 14)
  3. RMSE par zone (17 arrondissements)
  4. Amélioration (%) par métrique

Auteur : Personne B
Dépendance : evaluate_model.py + feature_importance.py + save_model.py doivent être terminés
"""

import json
import os
import matplotlib
matplotlib.use("Agg")   # mode non-interactif (pas de display requis)
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np

# ============================================================
# CHEMINS
# ============================================================
LOCAL_METRICS    = "/home/jovyan/work/ml/metrics.json"
LOCAL_IMPORTANCE = "/home/jovyan/work/ml/feature_importance.json"
OUTPUT_DIR       = "/home/jovyan/work/ml/plots"

os.makedirs(OUTPUT_DIR, exist_ok=True)

# ============================================================
# PALETTE COULEURS
# ============================================================
COLOR_MODEL    = "#1D9E75"   # teal  — modèle GBT
COLOR_BASELINE = "#D85A30"   # coral — baseline
COLOR_BARS     = "#378ADD"   # blue  — feature importance
COLOR_ZONES    = "#7F77DD"   # purple — zones
COLOR_BG       = "#F8F8F6"
COLOR_TEXT     = "#2C2C2A"
COLOR_GRID     = "#D3D1C7"


def set_style():
    plt.rcParams.update({
        "figure.facecolor":  COLOR_BG,
        "axes.facecolor":    COLOR_BG,
        "axes.edgecolor":    COLOR_GRID,
        "axes.grid":         True,
        "grid.color":        COLOR_GRID,
        "grid.linewidth":    0.6,
        "grid.alpha":        0.8,
        "text.color":        COLOR_TEXT,
        "axes.labelcolor":   COLOR_TEXT,
        "xtick.color":       COLOR_TEXT,
        "ytick.color":       COLOR_TEXT,
        "font.family":       "DejaVu Sans",
        "font.size":         11,
        "axes.titlesize":    13,
        "axes.titleweight":  "bold",
        "axes.spines.top":   False,
        "axes.spines.right": False,
    })


# ============================================================
# GRAPHIQUE 1 — Comparaison RMSE / MAE / R²
# ============================================================
def plot_metrics_comparison(metrics: dict):
    print("[Graph 1] Comparaison métriques Modèle vs Baseline...")

    fig, axes = plt.subplots(1, 3, figsize=(14, 5))
    fig.patch.set_facecolor(COLOR_BG)
    fig.suptitle("GBT Modèle vs Baseline naïve (demand_lag_7d)",
                 fontsize=15, fontweight="bold", y=1.02, color=COLOR_TEXT)

    metrics_info = [
        ("RMSE",  metrics["gbt_rmse"],      metrics["baseline_rmse"],  "Erreur quadratique\n(plus bas = meilleur)"),
        ("MAE",   metrics["gbt_mae"],        metrics["baseline_mae"],   "Erreur absolue\n(plus bas = meilleur)"),
        ("R²",    metrics["gbt_r2"],         metrics["baseline_r2"],    "Variance expliquée\n(plus haut = meilleur)"),
    ]

    for ax, (name, gbt_val, base_val, subtitle) in zip(axes, metrics_info):
        bars = ax.bar(
            ["GBT\nModèle", "Baseline\n(lag_7d)"],
            [gbt_val, base_val],
            color=[COLOR_MODEL, COLOR_BASELINE],
            width=0.45,
            edgecolor="white",
            linewidth=0.8,
        )

        # Valeurs sur les barres
        for bar, val in zip(bars, [gbt_val, base_val]):
            ax.text(
                bar.get_x() + bar.get_width() / 2,
                bar.get_height() + max(gbt_val, base_val) * 0.02,
                f"{val:.4f}",
                ha="center", va="bottom",
                fontsize=12, fontweight="bold", color=COLOR_TEXT
            )

        ax.set_title(name, fontsize=14, fontweight="bold", pad=10)
        ax.set_xlabel(subtitle, fontsize=10, color="#5F5E5A")
        ax.set_ylim(0, max(gbt_val, base_val) * 1.25)
        ax.tick_params(axis="x", labelsize=11)
        ax.set_axisbelow(True)

        # Badge amélioration
        if name != "R²":
            improvement = (base_val - gbt_val) / base_val * 100
            ax.text(
                0.5, 0.92,
                f"▼ {improvement:.1f}% meilleur",
                transform=ax.transAxes,
                ha="center", fontsize=10,
                color=COLOR_MODEL, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#E1F5EE",
                          edgecolor=COLOR_MODEL, linewidth=0.8)
            )
        else:
            improvement = (gbt_val - base_val) * 100
            ax.text(
                0.5, 0.92,
                f"▲ +{improvement:.1f} pts",
                transform=ax.transAxes,
                ha="center", fontsize=10,
                color=COLOR_MODEL, fontweight="bold",
                bbox=dict(boxstyle="round,pad=0.3", facecolor="#E1F5EE",
                          edgecolor=COLOR_MODEL, linewidth=0.8)
            )

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "1_metrics_comparison.png")
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=COLOR_BG)
    plt.close()
    print(f"      → Sauvegardé : {path}")
    return path


# ============================================================
# GRAPHIQUE 2 — Feature Importance
# ============================================================
def plot_feature_importance(importance: dict):
    print("[Graph 2] Feature Importance...")

    features = [item["feature"]    for item in importance["all_features"]]
    scores   = [item["importance"] for item in importance["all_features"]]

    # Couleurs : top 3 en teal, reste en bleu clair
    colors = [COLOR_MODEL if i < 3 else COLOR_BARS
              for i in range(len(features))]

    fig, ax = plt.subplots(figsize=(12, 7))
    fig.patch.set_facecolor(COLOR_BG)

    bars = ax.barh(features[::-1], scores[::-1],
                   color=colors[::-1],
                   edgecolor="white", linewidth=0.6)

    # Labels valeurs
    for bar, score in zip(bars, scores[::-1]):
        ax.text(
            bar.get_width() + 0.003,
            bar.get_y() + bar.get_height() / 2,
            f"{score*100:.1f}%",
            va="center", ha="left",
            fontsize=10, color=COLOR_TEXT
        )

    ax.set_title("Importance des features — GBTRegressor",
                 fontsize=14, fontweight="bold", pad=12)
    ax.set_xlabel("Importance (réduction de variance)", fontsize=11)
    ax.set_xlim(0, max(scores) * 1.25)
    ax.set_axisbelow(True)

    # Légende top 3
    top3_patch  = mpatches.Patch(color=COLOR_MODEL, label="Top 3 prédicteurs")
    rest_patch  = mpatches.Patch(color=COLOR_BARS,  label="Autres features")
    ax.legend(handles=[top3_patch, rest_patch],
              loc="lower right", fontsize=10,
              framealpha=0.7, edgecolor=COLOR_GRID)

    # Annotation top 1
    top_feature = importance["all_features"][0]
    ax.annotate(
        f"  {top_feature['feature']}\n  {top_feature['importance']*100:.1f}% d'importance",
        xy=(top_feature["importance"], len(features) - 1),
        xytext=(top_feature["importance"] * 0.6, len(features) - 1 - 2),
        fontsize=10, color=COLOR_MODEL,
        arrowprops=dict(arrowstyle="->", color=COLOR_MODEL, lw=1.2)
    )

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "2_feature_importance.png")
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=COLOR_BG)
    plt.close()
    print(f"      → Sauvegardé : {path}")
    return path


# ============================================================
# GRAPHIQUE 3 — RMSE par zone
# ============================================================
def plot_zone_comparison():
    """Lit zone_comparison.json local ou reconstruit depuis les données connues."""
    print("[Graph 3] RMSE par zone...")

    # Données du run (issues de save_model.py B12)
    zones_data = [
        {"zone_id": 0,  "avg_demand": 1.37,  "rmse": 0.6920},
        {"zone_id": 1,  "avg_demand": 10.63, "rmse": 7.0735},
        {"zone_id": 2,  "avg_demand": 10.49, "rmse": 6.8355},
        {"zone_id": 3,  "avg_demand": 25.00, "rmse": 12.3682},
        {"zone_id": 4,  "avg_demand": 6.00,  "rmse": 3.3081},
        {"zone_id": 5,  "avg_demand": 5.15,  "rmse": 2.7932},
        {"zone_id": 6,  "avg_demand": 2.95,  "rmse": 1.6639},
        {"zone_id": 7,  "avg_demand": 4.63,  "rmse": 2.6479},
        {"zone_id": 8,  "avg_demand": 12.96, "rmse": 6.6622},
        {"zone_id": 9,  "avg_demand": 3.17,  "rmse": 1.7813},
        {"zone_id": 10, "avg_demand": 3.73,  "rmse": 2.1410},
        {"zone_id": 11, "avg_demand": 5.55,  "rmse": 3.1426},
        {"zone_id": 12, "avg_demand": 1.05,  "rmse": 0.6275},
        {"zone_id": 13, "avg_demand": 1.95,  "rmse": 1.3105},
        {"zone_id": 14, "avg_demand": 1.92,  "rmse": 1.0839},
        {"zone_id": 15, "avg_demand": 3.62,  "rmse": 2.1650},
        {"zone_id": 16, "avg_demand": 3.76,  "rmse": 2.3012},
    ]

    zone_ids = [f"Zone {d['zone_id']}" for d in zones_data]
    rmses    = [d["rmse"]             for d in zones_data]
    demands  = [d["avg_demand"]       for d in zones_data]

    # Couleur par niveau de difficulté
    def get_color(rmse):
        if rmse < 2:    return "#1D9E75"   # teal = facile
        elif rmse < 6:  return "#EF9F27"   # amber = moyen
        else:           return "#D85A30"   # coral = difficile

    colors = [get_color(r) for r in rmses]

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 10),
                                    gridspec_kw={"height_ratios": [2, 1]})
    fig.patch.set_facecolor(COLOR_BG)
    fig.suptitle("Performance du modèle par zone — Test set (Mai–Jun 2014)",
                 fontsize=15, fontweight="bold", y=1.01, color=COLOR_TEXT)

    # ── Graphique haut : RMSE par zone ───────────────────────
    x = np.arange(len(zone_ids))
    bars = ax1.bar(x, rmses, color=colors, edgecolor="white",
                   linewidth=0.6, width=0.7)

    # Ligne RMSE global
    rmse_global = 5.0621
    ax1.axhline(rmse_global, color=COLOR_BARS, linewidth=1.5,
                linestyle="--", alpha=0.8,
                label=f"RMSE global = {rmse_global}")

    for bar, rmse in zip(bars, rmses):
        ax1.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.15,
            f"{rmse:.1f}",
            ha="center", va="bottom", fontsize=8, color=COLOR_TEXT
        )

    ax1.set_xticks(x)
    ax1.set_xticklabels(zone_ids, rotation=45, ha="right", fontsize=9)
    ax1.set_ylabel("RMSE", fontsize=11)
    ax1.set_title("RMSE par zone (arrondissement)", fontsize=12)
    ax1.set_axisbelow(True)
    ax1.legend(fontsize=10)

    # Légende couleurs
    easy   = mpatches.Patch(color="#1D9E75", label="Facile  (RMSE < 2)")
    medium = mpatches.Patch(color="#EF9F27", label="Moyen   (RMSE 2–6)")
    hard   = mpatches.Patch(color="#D85A30", label="Difficile (RMSE > 6)")
    ax1.legend(handles=[easy, medium, hard,
                        plt.Line2D([0], [0], color=COLOR_BARS,
                                   linewidth=1.5, linestyle="--",
                                   label=f"RMSE global = {rmse_global}")],
               fontsize=9, loc="upper left")

    # ── Graphique bas : demande moyenne par zone ──────────────
    ax2.bar(x, demands, color=COLOR_ZONES, edgecolor="white",
            linewidth=0.6, width=0.7, alpha=0.8)
    ax2.set_xticks(x)
    ax2.set_xticklabels(zone_ids, rotation=45, ha="right", fontsize=9)
    ax2.set_ylabel("Demande moy\n(taxis/slot)", fontsize=10)
    ax2.set_title("Demande moyenne par zone", fontsize=12)
    ax2.set_axisbelow(True)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "3_zone_rmse.png")
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=COLOR_BG)
    plt.close()
    print(f"      → Sauvegardé : {path}")
    return path


# ============================================================
# GRAPHIQUE 4 — Résumé amélioration en %
# ============================================================
def plot_improvement_summary(metrics: dict):
    print("[Graph 4] Résumé amélioration...")

    categories  = ["RMSE\n(−33.6%)", "MAE\n(−38.0%)", "R²\n(+47.4 pts)"]
    gbt_vals    = [metrics["gbt_rmse"],  metrics["gbt_mae"],  metrics["gbt_r2"]]
    base_vals   = [metrics["baseline_rmse"], metrics["baseline_mae"], metrics["baseline_r2"]]

    fig, ax = plt.subplots(figsize=(10, 5))
    fig.patch.set_facecolor(COLOR_BG)

    x     = np.arange(len(categories))
    width = 0.32

    bars1 = ax.bar(x - width/2, gbt_vals,  width, label="GBT Modèle",
                   color=COLOR_MODEL,   edgecolor="white", linewidth=0.8)
    bars2 = ax.bar(x + width/2, base_vals, width, label="Baseline (lag_7d)",
                   color=COLOR_BASELINE, edgecolor="white", linewidth=0.8)

    # Valeurs
    for bar in list(bars1) + list(bars2):
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            bar.get_height() + 0.05,
            f"{bar.get_height():.4f}",
            ha="center", va="bottom", fontsize=9, color=COLOR_TEXT
        )

    # Flèches d'amélioration
    improvements = [
        metrics["rmse_improvement_pct"],
        metrics["mae_improvement_pct"],
        (metrics["gbt_r2"] - metrics["baseline_r2"]) * 100
    ]
    labels_arrow = [f"−{v:.1f}%" if i < 2 else f"+{v:.1f} pts"
                    for i, v in enumerate(improvements)]

    for i, (label, gbt_v, base_v) in enumerate(
            zip(labels_arrow, gbt_vals, base_vals)):
        y_max = max(gbt_v, base_v)
        ax.annotate(
            "", xy=(i - width/2, gbt_v), xytext=(i + width/2, base_v),
            arrowprops=dict(arrowstyle="<->", color="#444441", lw=1.5)
        )
        ax.text(i, y_max * 1.10, label,
                ha="center", fontsize=11, fontweight="bold",
                color=COLOR_MODEL,
                bbox=dict(boxstyle="round,pad=0.25",
                          facecolor="#E1F5EE",
                          edgecolor=COLOR_MODEL, linewidth=0.8))

    ax.set_xticks(x)
    ax.set_xticklabels(categories, fontsize=12)
    ax.set_title("Amélioration du modèle GBT par rapport à la baseline",
                 fontsize=14, fontweight="bold", pad=12)
    ax.legend(fontsize=11, loc="upper right")
    ax.set_ylim(0, max(base_vals) * 1.35)
    ax.set_axisbelow(True)

    plt.tight_layout()
    path = os.path.join(OUTPUT_DIR, "4_improvement_summary.png")
    plt.savefig(path, dpi=150, bbox_inches="tight",
                facecolor=COLOR_BG)
    plt.close()
    print(f"      → Sauvegardé : {path}")
    return path


# ============================================================
# MAIN
# ============================================================
def main():
    print("=" * 60)
    print("  TaaSim — Visualisation des résultats ML")
    print("=" * 60)

    set_style()

    # Chargement métriques
    with open(LOCAL_METRICS, "r") as f:
        metrics = json.load(f)

    with open(LOCAL_IMPORTANCE, "r") as f:
        importance = json.load(f)

    # Génération des 4 graphiques
    p1 = plot_metrics_comparison(metrics)
    p2 = plot_feature_importance(importance)
    p3 = plot_zone_comparison()
    p4 = plot_improvement_summary(metrics)

    print("\n" + "=" * 60)
    print("  ✅ 4 graphiques générés dans :")
    print(f"     {OUTPUT_DIR}/")
    print("     1_metrics_comparison.png")
    print("     2_feature_importance.png")
    print("     3_zone_rmse.png")
    print("     4_improvement_summary.png")
    print("=" * 60)


if __name__ == "__main__":
    main()