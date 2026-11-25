"""Produit les figures à partir des résultats exportés des jobs Spark."""

import argparse
import json
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd
from matplotlib.ticker import FuncFormatter, MaxNLocator

COLORS = {"ink": "#17273a", "blue": "#126982", "amber": "#a85124", "grid": "#d9e1e7"}
NAMES = {
    "Persistence": "Persistance",
    "WeeklyWithPersistenceFallback": "Hebdomadaire",
    "LinearRegression": "Linéaire",
    "DecisionTreeRegressor": "Arbre",
    "RandomForestRegressor": "Forêt",
}


def number(value, _):
    return f"{value:,.0f}".replace(",", "\u202f")


def dates(axis):
    locator = mdates.AutoDateLocator(minticks=4, maxticks=8)
    axis.xaxis.set_major_locator(locator)
    axis.xaxis.set_major_formatter(
        mdates.ConciseDateFormatter(
            locator,
            formats=["%Y", "%m/%Y", "%d/%m", "%H:%M", "%H:%M", "%S"],
            zero_formats=["", "%Y", "%m/%Y", "%d/%m", "%H:%M", "%H:%M"],
            show_offset=False,
        )
    )


def save(fig, path):
    fig.savefig(path, metadata={"Date": None})
    plt.close(fig)
    if path.suffix == ".svg":
        lines = path.read_text(encoding="utf-8").splitlines()
        path.write_text(
            "\n".join(line.rstrip() for line in lines) + "\n",
            encoding="utf-8",
            newline="\n",
        )


def profile_figure(profile, gold_daily, panel_daily, period, output):
    fig, axes = plt.subplots(3, 1, figsize=(9, 8), layout="constrained")
    fig.suptitle("Wikimedia · Couverture et distribution des vues", fontweight="bold")
    for name, title, color in (
        ("all", "Gold complet", COLORS["ink"]),
        ("panel", "20 pages, historique disponible", COLORS["blue"]),
    ):
        stats = profile[name]
        axes[0].plot(
            [q * 100 for q in stats["views_quantile_probabilities"]],
            stats["views_quantiles"],
            marker="o",
            markersize=4,
            label=title,
            color=color,
        )
    axes[0].set(
        title="Quantiles des vues par page-jour", xlabel="Percentile", ylabel="Vues"
    )
    axes[0].set_yscale("symlog", linthresh=10)
    axes[0].legend(frameon=False)
    axes[1].plot(
        gold_daily.event_date, gold_daily.pages_count, color=COLORS["blue"], lw=0.7
    )
    axes[1].axhline(100, color=COLORS["amber"], ls="--", lw=1)
    axes[1].set(title="Pages observées par jour dans Gold", ylabel="Pages")
    dates(axes[1])
    dense = panel_daily[panel_daily.event_date.between(*period)]
    axes[2].plot(dense.event_date, dense.total_views, color=COLORS["blue"], lw=1)
    axes[2].set(title="Total quotidien des vues sur le panel retenu", ylabel="Vues")
    axes[2].yaxis.set_major_formatter(FuncFormatter(number))
    dates(axes[2])
    save(fig, output)


def comparisons(metrics, split):
    baselines = [
        (NAMES[key], value) for key, value in metrics[f"{split}_baselines"].items()
    ]
    if split == "validation":
        initial = metrics["validation_initial"]
        leaders = [
            min(
                (trial for trial in initial if trial["algorithm"] == name),
                key=lambda t: (t["metrics"]["mae"], t["metrics"]["rmse"]),
            )
            for name in list(NAMES)[2:]
        ]
        challenger = min(
            metrics["validation_tuning"],
            key=lambda t: (t["metrics"]["mae"], t["metrics"]["rmse"]),
        )
    else:
        leaders = metrics["test_initial"]
        challenger = metrics["test_tuned_challenger"]
    return [
        *baselines,
        *((NAMES[t["algorithm"]], t["metrics"]) for t in leaders),
        (NAMES[challenger["algorithm"]] + " (réglage)", challenger["metrics"]),
    ]


def comparison_figure(metrics, output):
    fig, axes = plt.subplots(2, 2, figsize=(11, 6), layout="constrained")
    fig.suptitle(
        "Comparaison des erreurs · mêmes observations par partition", fontweight="bold"
    )
    for axes_row, split, title in zip(
        axes, ("validation", "test"), ("Validation", "Test final")
    ):
        comparison = comparisons(metrics, split)
        for axis, metric in zip(axes_row, ("mae", "rmse")):
            labels = [name for name, _ in comparison]
            values = [data[metric] for _, data in comparison]
            axis.barh(
                labels,
                values,
                color=[COLORS["ink"]] * 2 + [COLORS["blue"]] * 3 + [COLORS["amber"]],
                height=0.62,
            )
            axis.invert_yaxis()
            axis.set(title=f"{title} · {metric.upper()}", xlabel="Erreur en vues")
            axis.xaxis.set_major_formatter(FuncFormatter(number))
            axis.xaxis.set_major_locator(MaxNLocator(nbins=6))
    save(fig, output)


def predictions_figure(values, metrics, page, output):
    importance = metrics.get("feature_importance")
    fig, axes = plt.subplots(
        3 if importance else 2,
        1,
        figsize=(9, 8 if importance else 6),
        layout="constrained",
    )
    fig.suptitle("Prédictions et erreurs sur le test final", fontweight="bold")
    page_values = values[values.page_title == page].sort_values("prediction_date")
    if page_values.empty:
        raise ValueError(f"Page absente des prédictions enregistrées : {page}")
    axes[0].plot(
        page_values.prediction_date,
        page_values.target_next_day_views,
        label="Réel",
        color=COLORS["ink"],
        lw=1.5,
    )
    axes[0].plot(
        page_values.prediction_date,
        page_values.predicted_views,
        label="Prédit",
        color=COLORS["amber"],
        lw=1.5,
    )
    axes[0].set(title=page.replace("_", " "), ylabel="Vues")
    axes[0].yaxis.set_major_formatter(FuncFormatter(number))
    axes[0].legend(frameon=False)
    dates(axes[0])
    errors = values.predicted_views - values.target_next_day_views
    axes[1].hist(errors, bins=45, color=COLORS["blue"])
    axes[1].axvline(0, color=COLORS["ink"], lw=1)
    axes[1].set(xlabel="Prédiction − réel (vues)", ylabel="Observations", yscale="log")
    axes[1].xaxis.set_major_formatter(FuncFormatter(number))
    if importance:
        ranked = sorted(importance.items(), key=lambda item: item[1])[-10:]
        axes[2].barh(
            [c for c, _ in ranked], [v for _, v in ranked], color=COLORS["blue"]
        )
        axes[2].set_xlabel("Importance relative dans le modèle")
    save(fig, output)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", required=True, type=Path)
    parser.add_argument("--model-results", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--example-page", default="ChatGPT")
    args = parser.parse_args()
    plt.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "text.color": COLORS["ink"],
            "axes.labelcolor": COLORS["ink"],
            "axes.spines.top": False,
            "axes.spines.right": False,
            "axes.grid": True,
            "axes.axisbelow": True,
            "grid.color": COLORS["grid"],
            "grid.linewidth": 0.65,
            "svg.hashsalt": "trendcast",
        }
    )
    profile = json.loads(
        next((args.profile / "manifest").glob("part-*")).read_text("utf-8")
    )
    metrics = json.loads(
        next((args.model_results / "manifest").glob("part-*")).read_text("utf-8")
    )
    daily = {
        name: pd.read_csv(
            next((args.profile / name).glob("part-*.csv")), parse_dates=["event_date"]
        )
        for name in ("gold_daily", "panel_daily")
    }
    predictions = pd.read_parquet(args.model_results / "test_predictions")
    predictions["prediction_date"] = pd.to_datetime(predictions.prediction_date)
    period = tuple(
        metrics["dataset_manifest"][key]
        for key in ("first_input_day", "last_input_day")
    )
    args.output.mkdir(parents=True, exist_ok=True)
    profile_figure(
        profile,
        daily["gold_daily"],
        daily["panel_daily"],
        period,
        args.output / "exploration.svg",
    )
    comparison_figure(metrics, args.output / "comparaison.svg")
    predictions_figure(
        predictions, metrics, args.example_page, args.output / "predictions.svg"
    )


if __name__ == "__main__":
    main()
