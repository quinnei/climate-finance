from __future__ import annotations

from functools import lru_cache
from pathlib import Path
import textwrap

import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import numpy as np
import pandas as pd
from matplotlib.colors import LinearSegmentedColormap
from statsmodels.nonparametric.smoothers_lowess import lowess


PROJECT_ROOT = Path(__file__).resolve().parents[2]
FINAL_DATA_DIR = PROJECT_ROOT / "1_Data" / "final"
PIVOT_DIR = PROJECT_ROOT / "1_Data" / "pivot_tables"

DATA_PATH = FINAL_DATA_DIR / "[FINAL] Climate Finance (2015-2023).csv"
PER_CAPITA_PATH = FINAL_DATA_DIR / "[FINAL] Climate Finance PER CAPITA.csv"

PIVOT_FILES = {
    "annual_total_commitment": PIVOT_DIR / "[SUMMARY] annual_total_commitment.xlsx",
    "provider_total_commitment": PIVOT_DIR / "[SUMMARY] provider_total_commitment.xlsx",
    "provider_yearly_commitment": PIVOT_DIR / "[SUMMARY] provider_yearly_commitment.xlsx",
    "recipient_total_commitment": PIVOT_DIR / "[SUMMARY] recipient_total_commitment.xlsx",
    "recipient_per_capita_yearly": PIVOT_DIR / "[SUMMARY] recipient_per_capita_yearly.xlsx",
    "region_concessionality_share": PIVOT_DIR / "[SUMMARY] region_concessionality_share.xlsx",
}

COLOR_MAPS = {
    "provider_type": {
        "Bilateral donor (DAC)": "#0077b6",
        "Bilateral donor (Non-DAC)": "#2ca02c",
        "Multilateral donor (Non-MDB)": "#8338ec",
        "Multilateral donor (MDB)": "#ffbe0b",
        "Private donor (NGOs/Philanthropy/Charity)": "#d62828",
    },
    "concessionality": {
        "Concessional, Development as the main objective": "#1f77b4",
        "Concessional, From NGOs/philanthropy/charity": "#2ca02c",
        "Non-concessional, Development NOT the main objective": "#d62728",
        "Non-concessional, Export credits": "#ff9f1c",
        "Non-concessional, Financial instruments to promote private sector development in developing countries": "#7f3c8d",
        "Unspecified": "#4d4d4d",
    },
}

REGION_ORDER = [
    "Africa",
    "Asia",
    "Latin America & the Caribbean",
    "Europe",
    "Middle East",
    "Oceania",
    "Developing countries (General; Unspecified)",
]

PROVIDER_ALIASES = {
    "EU institutions (EIB)": ["EU institutions (EIB)", "European Investment Bank (EIB)"],
    "European Investment Bank (EIB)": ["European Investment Bank (EIB)", "EU institutions (EIB)"],
    "EU Institutions (excl. EIB)": ["EU Institutions (excl. EIB)", "EU Institutions (Excluding EIB)"],
    "EU Institutions (Excluding EIB)": ["EU Institutions (Excluding EIB)", "EU Institutions (excl. EIB)"],
}

PROVIDER_REGION_ORDER = [
    "Africa",
    "Asia",
    "Latin America & the Caribbean",
    "Europe",
    "Middle East",
    "Oceania",
    "Developing countries (General; Unspecified)",
]

PROVIDER_REGION_COLORS = {
    "Africa": "#d62728",
    "Asia": "#2ca02c",
    "Latin America & the Caribbean": "#1f77b4",
    "Europe": "#ff9f1c",
    "Middle East": "#7f3c8d",
    "Oceania": "#b58900",
    "Developing countries (General; Unspecified)": "#4d4d4d",
}

INCOME_COLORS = {
    "LDCs": "#d62828",
    "Other LICs": "#f77f00",
    "LMICs": "#2a9d8f",
    "UMICs": "#457b9d",
    "MADCTs": "#6a4c93",
    "Unallocated": "#7a7a7a",
    "Unspecified": "#9e9e9e",
}


@lru_cache(maxsize = 1)
def _read_main() -> pd.DataFrame:
    return pd.read_csv(DATA_PATH)


@lru_cache(maxsize = 1)
def _read_per_capita() -> pd.DataFrame:
    return pd.read_csv(PER_CAPITA_PATH)


@lru_cache(maxsize = None)
def _read_pivot(name: str) -> pd.DataFrame:
    return pd.read_excel(PIVOT_FILES[name])


def load_frames() -> tuple[pd.DataFrame, pd.DataFrame]:
    return _read_main().copy(), _read_per_capita().copy()


def _bn(series: pd.Series) -> pd.Series:
    return series / 1_000_000


def _style_axis(
    ax,
    xlabel: str | None = None,
    ylabel: str | None = None,
    axis_label_size: float | None = None,
    tick_label_size: float | None = None,
) -> None:
    ax.spines["top"].set_visible(False)
    ax.spines["right"].set_visible(False)
    ax.grid(axis = "y", color = "#d9d9d9", linewidth = 0.8, alpha = 0.7)
    ax.set_axisbelow(True)
    if xlabel is not None:
        ax.set_xlabel(xlabel, fontsize = axis_label_size) if axis_label_size is not None else ax.set_xlabel(xlabel)
    if ylabel is not None:
        ax.set_ylabel(ylabel, fontsize = axis_label_size) if axis_label_size is not None else ax.set_ylabel(ylabel)
    if tick_label_size is not None:
        ax.tick_params(axis = "both", labelsize = tick_label_size)


def _wrap(value: str, width: int) -> str:
    return "\n".join(textwrap.wrap(str(value), width = width))


def _break_before_parenthesis(value: str) -> str:
    return str(value).replace(" (", "\n(")


def get_provider_data(df: pd.DataFrame, provider: str) -> pd.DataFrame:
    aliases = PROVIDER_ALIASES.get(provider, [provider])
    return df[df["provider"].isin(aliases)].copy()


def _share_of_commitment(df: pd.DataFrame, category: str) -> pd.DataFrame:
    grouped = (
        df.groupby(["year", category], as_index = False)["commitment"]
        .sum()
        .sort_values(["year", category])
    )
    grouped["share_pct"] = grouped.groupby("year")["commitment"].transform(
        lambda series: series / series.sum() * 100 if series.sum() else 0
    )
    years = sorted(df["year"].dropna().unique())
    categories = list(grouped[category].drop_duplicates())
    full_index = pd.MultiIndex.from_product([years, categories], names = ["year", category]).to_frame(index = False)
    return full_index.merge(grouped, on = ["year", category], how = "left")


def _share_of_commitment_total(df: pd.DataFrame, category: str) -> pd.DataFrame:
    grouped = (
        df.groupby(category, as_index = False)["commitment"]
        .sum()
        .sort_values("commitment", ascending = False)
    )
    total_commitment = grouped["commitment"].sum()
    grouped["share_pct"] = grouped["commitment"] / total_commitment * 100 if total_commitment else 0
    return grouped


def _exclude_unearmarked_recipient_categories(df: pd.DataFrame) -> pd.DataFrame:
    exclusion_mask = (
        df["recipient"].isin(REGION_ORDER)
        | df["recipient"].astype(str).str.contains(
            r"regional|subregional|developing countries|unspecified",
            case = False,
            na = False,
        )
    )
    return df.loc[~exclusion_mask].copy()


def _provider_region_share_frame(df: pd.DataFrame, provider: str) -> pd.DataFrame:
    provider_df = get_provider_data(df, provider)
    provider_data = provider_df.groupby(["year", "region"], as_index = False)["commitment"].sum()
    provider_data["share_pct"] = provider_data.groupby("year")["commitment"].transform(
        lambda series: series / series.sum() * 100 if series.sum() else 0
    )
    provider_data["region_label"] = provider_data["region"].replace(
        {"Developing countries (General; Unspecified)": "Developing countries\n(General; Unspecified)"}
    )
    return provider_data


def _provider_top_sectors_frame(df: pd.DataFrame, provider: str) -> pd.DataFrame:
    provider_df = get_provider_data(df, provider)
    grouped = (
        provider_df.groupby("sector", as_index = False)["commitment"]
        .sum()
        .sort_values("commitment", ascending = False)
        .head(10)
    )
    grouped.insert(0, "Rank", range(1, len(grouped) + 1))
    grouped["Commitment (Million USD)"] = (grouped["commitment"] / 1_000).round(2)
    return grouped.rename(columns = {"sector": "Sector"})[["Rank", "Sector", "Commitment (Million USD)"]]


def fig_total_commitment_per_year(df: pd.DataFrame):
    annual = _read_pivot("annual_total_commitment").copy()
    annual["commitment_bn_usd"] = _bn(annual["commitment"])
    fig, ax = plt.subplots(figsize = (9.5, 4.8))
    ax.plot(
        annual["year"],
        annual["commitment_bn_usd"],
        color = "#1f77b4",
        linewidth = 3,
        marker = "o",
        markersize = 7,
    )
    _style_axis(ax, "Year", "Commitment (Billion USD)")
    ax.set_xticks(sorted(annual["year"].unique()))
    ax.set_ylim(0, 160)
    ax.set_yticks(np.arange(0, 161, 20))
    ax.yaxis.set_major_formatter(mtick.StrMethodFormatter("{x:,.0f}"))
    return fig


def fig_region_share(df: pd.DataFrame):
    shares = _share_of_commitment(df, "region")
    region_order = [region for region in REGION_ORDER if region in shares["region"].unique()]
    heatmap_data = (
        shares.pivot(index = "region", columns = "year", values = "share_pct")
        .reindex(region_order)
        .fillna(0)
    )
    display_labels = [
        "Developing countries\n(General; Unspecified)"
        if region == "Developing countries (General; Unspecified)"
        else "Latin America &\nthe Caribbean"
        if region == "Latin America & the Caribbean"
        else region
        for region in heatmap_data.index
    ]
    cmap = LinearSegmentedColormap.from_list(
        "region_share",
        ["#f7fcf5", "#d9f0d3", "#a6dba0", "#5aae61", "#1b7837", "#00441b"],
    )
    fig, ax = plt.subplots(figsize = (9.4, 4.8))
    axis_label_size = 12
    tick_label_size = 10
    im = ax.imshow(heatmap_data.to_numpy(), aspect = "auto", cmap = cmap)
    ax.set_xticks(np.arange(len(heatmap_data.columns)))
    ax.set_xticklabels(heatmap_data.columns, fontsize = tick_label_size)
    ax.set_yticks(np.arange(len(display_labels)))
    ax.set_yticklabels(display_labels, fontsize = tick_label_size)
    ax.set_xticks(np.arange(-0.5, len(heatmap_data.columns), 1), minor = True)
    ax.set_yticks(np.arange(-0.5, len(display_labels), 1), minor = True)
    ax.grid(which = "minor", color = "#000000", linestyle = "-", linewidth = 1.1)
    ax.tick_params(which = "minor", bottom = False, left = False)
    ax.set_xlabel("Year", fontsize = axis_label_size)
    ax.set_ylabel("")
    value_threshold = heatmap_data.to_numpy().max() * 0.55
    for i in range(heatmap_data.shape[0]):
        for j in range(heatmap_data.shape[1]):
            value = heatmap_data.iloc[i, j]
            text_color = "white" if value >= value_threshold else "#1c1c1c"
            ax.text(j, i, f"{value:.1f}", ha = "center", va = "center", fontsize = 11, color = text_color, fontweight = "semibold")
    cbar = fig.colorbar(im, ax = ax, orientation = "vertical", fraction = 0.045, pad = 0.03)
    cbar.set_label("Share of commitment (%)", fontsize = axis_label_size)
    cbar.ax.tick_params(labelsize = tick_label_size)
    return fig


def fig_principal_objective_share(df: pd.DataFrame):
    metric_order = [
        "Climate is the principal objective",
        "Adaptation is the principal objective",
        "Mitigation is the principal objective",
    ]
    total_projects = df.groupby("year").size().rename("project_count")
    frames = []
    for column, label in [
        ("climate_obj", "Climate is the principal objective"),
        ("adaptation_obj", "Adaptation is the principal objective"),
        ("mitigation_obj", "Mitigation is the principal objective"),
    ]:
        principal = (
            df[df[column].eq("Primary")]
            .groupby("year")
            .size()
            .rename("principal_projects")
            .reset_index()
        )
        principal["metric"] = label
        frames.append(principal)
    principal_df = pd.concat(frames, ignore_index = True).merge(total_projects, on = "year", how = "left")
    principal_df["share_pct"] = principal_df["principal_projects"] / principal_df["project_count"] * 100

    colors = {
        "Climate is the principal objective": "#1f77b4",
        "Adaptation is the principal objective": "#2ca02c",
        "Mitigation is the principal objective": "#f4a261",
    }
    fig, ax = plt.subplots(figsize = (9.5, 4.8))
    handles = []
    for metric in metric_order:
        group = principal_df[principal_df["metric"].eq(metric)]
        ax.plot(group["year"], group["share_pct"], marker = "o", linewidth = 3, label = metric, color = colors[metric])
        handles.append(ax.lines[-1])
    _style_axis(ax, "Year", "Share of projects (%)")
    ax.set_xticks(sorted(principal_df["year"].unique()))
    ax.set_ylim(0, 40)
    ax.set_yticks(np.arange(0, 41, 5))
    ax.legend(handles, metric_order, frameon = False, fontsize = 10.5)
    return fig


def fig_principal_objective_commitment(df: pd.DataFrame):
    metric_order = [
        "Climate is the principal objective",
        "Adaptation is the principal objective",
        "Mitigation is the principal objective",
    ]
    frames = []
    for column, label in [
        ("climate_obj", "Climate is the principal objective"),
        ("adaptation_obj", "Adaptation is the principal objective"),
        ("mitigation_obj", "Mitigation is the principal objective"),
    ]:
        principal = df[df[column].eq("Primary")].groupby("year", as_index = False)["commitment"].sum()
        principal["metric"] = label
        principal["commitment_bn_usd"] = _bn(principal["commitment"])
        frames.append(principal)
    summary = pd.concat(frames, ignore_index = True)
    colors = {
        "Climate is the principal objective": "#1f77b4",
        "Adaptation is the principal objective": "#2ca02c",
        "Mitigation is the principal objective": "#f4a261",
    }
    fig, ax = plt.subplots(figsize = (9.5, 4.8))
    handles = []
    for metric in metric_order:
        group = summary[summary["metric"].eq(metric)]
        ax.plot(group["year"], group["commitment_bn_usd"], marker = "o", linewidth = 3, label = metric, color = colors[metric])
        handles.append(ax.lines[-1])
    _style_axis(ax, "Year", "Commitment (Billion USD)")
    ax.set_xticks(sorted(summary["year"].unique()))
    ax.set_ylim(bottom = 0)
    ax.legend(handles, metric_order, frameon = False, fontsize = 10.5)
    ax.yaxis.set_major_formatter(mtick.StrMethodFormatter("{x:,.0f}"))
    return fig


def fig_provider_type_share(df: pd.DataFrame):
    shares = _share_of_commitment(df, "provider_type")
    fig, ax = plt.subplots(figsize = (9.5, 5))
    for provider_type, group in shares.groupby("provider_type"):
        ax.plot(
            group["year"],
            group["share_pct"],
            marker = "o",
            linewidth = 3,
            label = provider_type,
            color = COLOR_MAPS["provider_type"][provider_type],
        )
    _style_axis(ax, "Year", "Share of commitment (%)")
    ax.set_xticks(sorted(shares["year"].unique()))
    ax.legend(frameon = False, fontsize = 10.5)
    return fig


def fig_provider_type_commitment_share_aggregate(df: pd.DataFrame):
    shares = _share_of_commitment_total(df, "provider_type").sort_values("share_pct")
    fig, ax = plt.subplots(figsize = (9.5, 4.0))
    y_labels = [_break_before_parenthesis(value) for value in shares["provider_type"]]
    ax.barh(
        y_labels,
        shares["share_pct"],
        color = [COLOR_MAPS["provider_type"][value] for value in shares["provider_type"]],
    )
    _style_axis(ax, "Share of commitment (%)", "", axis_label_size = 13, tick_label_size = 11)
    ax.grid(False)
    ax.grid(axis = "x", color = "#d9d9d9", linewidth = 0.8, alpha = 0.7)
    ax.set_xlim(0, max(100, shares["share_pct"].max() * 1.1))
    for y, value in enumerate(shares["share_pct"]):
        ax.text(value + 0.8, y, f"{value:.1f}%", va = "center", fontsize = 10)
    return fig


def donors_table_by_group(df: pd.DataFrame, donor_group: str) -> pd.DataFrame:
    pivot = _read_pivot("provider_total_commitment").copy()
    donor_groups = [
        ("Bilateral Donors", {"Bilateral donor (DAC)", "Bilateral donor (Non-DAC)"}),
        ("Multilateral Donors", {"Multilateral donor (MDB)", "Multilateral donor (Non-MDB)"}),
        ("Private Donors", {"Private donor (NGOs/Philanthropy/Charity)"}),
    ]
    for label, provider_types in donor_groups:
        if label != donor_group:
            continue
        ranked = (
            pivot[pivot["provider_type"].isin(provider_types)]
            .assign(commitment_bn_usd = lambda frame: _bn(frame["total_commitment"]))
            .nlargest(10, "commitment_bn_usd")
            .reset_index(drop = True)
        )
        ranked.insert(0, "Rank", range(1, len(ranked) + 1))
        ranked["Commitment"] = ranked["commitment_bn_usd"].map(lambda value: f"${value:,.2f}B")
        return ranked[["Rank", "provider", "Commitment"]].rename(columns = {"provider": "Provider"})
    raise ValueError(f"Unknown donor group: {donor_group}")


def fig_concessionality_share(df: pd.DataFrame):
    shares = _share_of_commitment(df, "concessionality")
    label_map = {
        "Non-concessional, Financial instruments to promote private sector development in developing countries": "Non-concessional, Private Sector Instruments",
    }
    shares["label"] = shares["concessionality"].replace(label_map)
    colors = {label_map.get(key, key): value for key, value in COLOR_MAPS["concessionality"].items()}
    fig, ax = plt.subplots(figsize = (10.0, 5.2))
    for label, group in shares.groupby("label"):
        ax.plot(group["year"], group["share_pct"], marker = "o", linewidth = 2.6, label = label, color = colors[label])
    _style_axis(ax, "Year", "Share of commitment (%)", axis_label_size = 11, tick_label_size = 10)
    ax.set_xticks(sorted(shares["year"].unique()))
    ax.set_ylim(0, 80)
    ax.legend(frameon = False, fontsize = 10.5, loc = "center right", bbox_to_anchor = (0.98, 0.26))
    return fig


def fig_region_concessionality(df: pd.DataFrame, region: str):
    grouped = _read_pivot("region_concessionality_share").copy()
    grouped = grouped[grouped["region"].eq(region)].rename(columns = {"percentage": "share_pct"})
    label_map = {
        "Non-concessional, Financial instruments to promote private sector development in developing countries": "Non-concessional, Private Sector Instruments",
    }
    grouped["label"] = grouped["concessionality"].replace(label_map)
    colors = {label_map.get(key, key): value for key, value in COLOR_MAPS["concessionality"].items()}
    fig, ax = plt.subplots(figsize = (10, 5.0))
    for label, group in grouped.groupby("label"):
        ax.plot(group["year"], group["share_pct"], marker = "o", linewidth = 2.6, label = label, color = colors[label])
    _style_axis(ax, "Year", "Share of commitment (%)", axis_label_size = 11, tick_label_size = 10)
    ax.set_xticks(sorted(grouped["year"].unique()))
    ax.set_ylim(0, 100)
    ax.legend(frameon = False, fontsize = 10.5, loc = "upper left")
    return fig


def fig_provider_region(df: pd.DataFrame, provider: str):
    grouped = _provider_region_share_frame(df, provider)
    fig_height = 4.3 if provider == "Japan" else 5
    fig, ax = plt.subplots(figsize = (9.5, fig_height))
    for region in PROVIDER_REGION_ORDER:
        region_slice = grouped[grouped["region"].eq(region) | grouped["region_label"].eq(region)]
        if region_slice.empty:
            continue
        ax.plot(
            region_slice["year"],
            region_slice["share_pct"],
            marker = "o",
            linewidth = 2.8,
            label = region.replace(" (General; Unspecified)", "\n(General; Unspecified)"),
            color = PROVIDER_REGION_COLORS[region],
        )
    _style_axis(ax, "Year", "Share of commitment (%)")
    ax.set_ylabel("Share of commitment (%)")
    ax.set_xticks(sorted(grouped["year"].unique()))
    ax.set_ylim(0, max(100, grouped["share_pct"].max() * 1.1))
    legend_kwargs = {"frameon": False, "fontsize": 10.5, "loc": "upper left"}
    if provider == "Japan":
        legend_kwargs = {"frameon": False, "fontsize": 9.8}
    if provider == "Germany":
        legend_kwargs = {"frameon": False, "fontsize": 10.5, "loc": "upper left", "bbox_to_anchor": (0.0, 1.08)}
    if provider == "France":
        legend_kwargs = {"frameon": False, "fontsize": 10.5, "loc": "upper left", "ncols": 1}
    if provider in {"EU Institutions (excl. EIB)", "EU Institutions (Excluding EIB)"}:
        legend_kwargs = {"frameon": False, "fontsize": 10.5, "loc": "upper left", "bbox_to_anchor": (0.0, 1.16)}
    ax.legend(**legend_kwargs)
    return fig


def provider_top_sectors_table(df: pd.DataFrame, provider: str) -> pd.DataFrame:
    ranked = _provider_top_sectors_frame(df, provider).copy()
    ranked["Commitment"] = (ranked["Commitment (Million USD)"] / 1000).map(lambda value: f"${value:,.2f}B")
    return ranked[["Rank", "Sector", "Commitment"]]


def fig_top10_recipients_total(per_capita: pd.DataFrame):
    grouped = _read_pivot("recipient_total_commitment").copy()
    grouped = (
        _exclude_unearmarked_recipient_categories(grouped)
        .dropna(subset = ["recipient", "total_commitment"])
        .sort_values("total_commitment", ascending = False)
        .head(10)
        .sort_values("total_commitment")
    )
    grouped["display"] = _bn(grouped["total_commitment"])
    fig, ax = plt.subplots(figsize = (9.5, 4.2))
    ax.barh(grouped["recipient"], grouped["display"], color = "#457b9d")
    _style_axis(ax, "Total commitment (Billion USD)", "", axis_label_size = 12, tick_label_size = 10)
    ax.grid(False)
    ax.xaxis.set_major_formatter(mtick.StrMethodFormatter("{x:,.0f}"))
    max_value = grouped["display"].max()
    ax.set_xlim(0, max_value * 1.14)
    ax.tick_params(axis = "y", labelsize = 10)
    for bar in ax.patches:
        width = bar.get_width()
        ax.text(width + max_value * 0.015, bar.get_y() + bar.get_height() / 2, f"${width:,.0f}B", va = "center", fontsize = 9.5)
    return fig


def fig_top10_recipients_per_capita(per_capita: pd.DataFrame):
    filtered = _read_pivot("recipient_per_capita_yearly").copy()
    filtered = filtered.drop_duplicates(subset = ["recipient"]).dropna(subset = ["recipient", "commitment_per_capita", "SIDS"]).copy()
    filtered = filtered[filtered["SIDS"].isin([0, 1])].copy()
    top10_sids = filtered[filtered["SIDS"].eq(1)].sort_values("commitment_per_capita", ascending = False).head(10).copy()
    top10_non = filtered[filtered["SIDS"].eq(0)].sort_values("commitment_per_capita", ascending = False).head(10).copy()
    label_map = {"Saint Vincent and the Grenadines": "Saint Vincent and\nthe Grenadines"}
    top10_sids["display_recipient"] = top10_sids["recipient"].replace(label_map)
    top10_non["display_recipient"] = top10_non["recipient"].replace(label_map)

    fig, axes = plt.subplots(2, 1, figsize = (9.5, 7.7), sharex = True)
    axes[0].barh(top10_sids["display_recipient"], top10_sids["commitment_per_capita"], color = "#2a9d8f")
    axes[1].barh(top10_non["display_recipient"], top10_non["commitment_per_capita"], color = "#eda962")
    axes[0].set_title("SIDS", loc = "left", fontsize = 12.5, fontweight = "bold")
    axes[1].set_title("Non-SIDS", loc = "left", fontsize = 12.5, fontweight = "bold")
    axes[0].invert_yaxis()
    axes[1].invert_yaxis()
    for ax in axes:
        _style_axis(ax, None, "", tick_label_size = 10)
        ax.grid(False)
        ax.tick_params(axis = "y", labelsize = 10)
    shared_x_max = max(
        top10_sids["commitment_per_capita"].max() if not top10_sids.empty else 0,
        top10_non["commitment_per_capita"].max() if not top10_non.empty else 0,
    )
    shared_x_upper = shared_x_max * 1.16 if shared_x_max else 1
    tickvals = list(range(0, 25001, 5000))
    ticklabels = ["0", "5k", "10k", "15k", "20k", "25k"]
    for ax in axes:
        ax.set_xlim(0, shared_x_upper)
        ax.set_xticks(tickvals)
        ax.set_xticklabels(ticklabels)
    axes[0].tick_params(axis = "x", labelbottom = True)
    for ax, scale in [(axes[0], shared_x_upper), (axes[1], shared_x_upper)]:
        for bar in ax.patches:
            width = bar.get_width()
            ax.text(
                min(width + scale * 0.01, scale * 0.965),
                bar.get_y() + bar.get_height() / 2,
                f"${width:,.0f}",
                va = "center",
                ha = "left",
                fontsize = 9.5,
                color = "#1c1c1c",
                clip_on = True,
            )
    axes[1].set_xlabel("Commitment per capita (USD)", fontsize = 12)
    fig.subplots_adjust(left = 0.29, right = 0.96, top = 0.93, bottom = 0.08, hspace = 0.22)
    return fig


def fig_recipient_mvi_scatter(per_capita: pd.DataFrame, lowess_frac: float = 0.70):
    mvi_columns = [
        "ECON_vulnerability",
        "ECON_lack_resilience",
        "ENV_vulnerability",
        "ENV_lack_resilience",
    ]
    scatter_df = per_capita.dropna(subset = mvi_columns + ["commitment_per_capita"]).copy()
    scatter_df = scatter_df[scatter_df["commitment_per_capita"] > 0].copy()
    scatter_df["log_commitment_per_capita"] = np.log(scatter_df["commitment_per_capita"])
    scatter_df["income"] = scatter_df["income"].fillna("Unspecified")
    dims = [
        ("ECON_vulnerability", "Economic vulnerability"),
        ("ECON_lack_resilience", "Economic lack of resilience"),
        ("ENV_vulnerability", "Environmental vulnerability"),
        ("ENV_lack_resilience", "Environmental lack of resilience"),
    ]
    fig, axes = plt.subplots(2, 2, figsize = (12.5, 9.5))
    axes = axes.flatten()
    y_median = scatter_df["log_commitment_per_capita"].median()
    axis_label_size = 14
    for ax, (column, label) in zip(axes, dims):
        for income, subset in scatter_df.groupby("income"):
            ax.scatter(
                subset[column],
                subset["log_commitment_per_capita"],
                s = 38,
                alpha = 0.8,
                color = INCOME_COLORS.get(income, "#7a7a7a"),
                edgecolor = "white",
                linewidth = 0.4,
                label = income,
            )
        fitted = scatter_df[[column, "log_commitment_per_capita"]].sort_values(column)
        smooth = lowess(fitted["log_commitment_per_capita"], fitted[column], frac = lowess_frac, return_sorted = True)
        ax.plot(smooth[:, 0], smooth[:, 1], color = "black", linewidth = 2.2)
        ax.axvline(scatter_df[column].median(), color = "#666666", linestyle = ":", linewidth = 1.1)
        ax.axhline(y_median, color = "#666666", linestyle = ":", linewidth = 1.1)
        ax.set_title(label, loc = "left", fontsize = 14, fontweight = "bold")
        ax.set_xlabel(label, fontsize = axis_label_size)
        ax.set_ylabel("Log(Commitment per capita)", fontsize = axis_label_size)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(color = "#d9d9d9", linewidth = 0.7, alpha = 0.65)
        ax.set_axisbelow(True)
        ax.tick_params(axis = "both", labelsize = 11)
    handles, labels = axes[0].get_legend_handles_labels()
    unique = dict(zip(labels, handles))
    ordered_labels = [label for label in ["LDCs", "Other LICs", "LMICs", "UMICs", "MADCTs"] if label in unique]
    ordered_handles = [unique[label] for label in ordered_labels]
    fig.legend(
        ordered_handles,
        ordered_labels,
        loc = "lower left",
        ncol = 5,
        frameon = False,
        bbox_to_anchor = (0.0, -0.08),
        fontsize = 12.5,
        title = "Income",
        title_fontsize = 12.5,
    )
    fig.subplots_adjust(top = 0.95, bottom = 0.22, hspace = 0.32, wspace = 0.22)
    return fig


def _escape_latex(value: object) -> str:
    text = str(value)
    replacements = {
        "\\": r"\textbackslash{}",
        "&": r"\&",
        "%": r"\%",
        "$": r"\$",
        "#": r"\#",
        "_": r"\_",
        "{": r"\{",
        "}": r"\}",
        "~": r"\textasciitilde{}",
        "^": r"\textasciicircum{}",
    }
    for source, target in replacements.items():
        text = text.replace(source, target)
    return text


def latex_table(frame: pd.DataFrame, caption: str, label: str) -> str:
    headers = [_escape_latex(value) for value in frame.columns.tolist()]
    rows = []
    for _, row in frame.iterrows():
        values = [_escape_latex(value) for value in row.tolist()]
        rows.append(" {} \\\\".format(" & ".join(values)))
    body = "\n".join(rows)
    return (
        "\\begin{center}\n"
        "{\\fontsize{10.5pt}{12.5pt}\\selectfont\n"
        "\\captionsetup{type=table}\n"
        "\\caption{" + _escape_latex(caption) + "}\n"
        "\\label{" + _escape_latex(label) + "}\n"
        "\\vspace{0.05em}\n"
        "\\renewcommand{\\arraystretch}{1.0}\n"
        "\\begin{adjustbox}{max width=\\linewidth}\n"
        "\\begin{tabular}{c l r}\n"
        "\\hline\n"
        + " & ".join(headers) + " \\\\\n"
        "\\hline\n"
        + body + "\n"
        "\\hline\n"
        "\\end{tabular}\n"
        "\\end{adjustbox}\n"
        "}\n"
        "\\end{center}\n"
    )
