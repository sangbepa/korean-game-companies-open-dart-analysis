#!/usr/bin/env python3
"""Build summary metrics, charts, and an executable Kaggle notebook."""

from __future__ import annotations

import argparse
import csv
import json
import os
import tempfile
from pathlib import Path
from typing import Any

os.environ.setdefault(
    "MPLCONFIGDIR", str(Path(tempfile.gettempdir()) / "game-company-analysis-mpl")
)

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt
import nbformat as nbf
import numpy as np
import pandas as pd
import seaborn as sns


PROJECT_ROOT = Path(__file__).resolve().parent
DEFAULT_DATASET_DIR = PROJECT_ROOT / "kaggle" / "dataset"
DEFAULT_CHART_DIR = PROJECT_ROOT / "analysis" / "charts"
DEFAULT_NOTEBOOK = (
    PROJECT_ROOT
    / "kaggle"
    / "notebook"
    / "korean_game_companies_financial_health.ipynb"
)

METRICS = {
    "revenue": ("IS", "매출액"),
    "operating_profit": ("IS", "영업이익"),
    "net_income": ("IS", "당기순이익(손실)"),
    "assets": ("BS", "자산총계"),
    "liabilities": ("BS", "부채총계"),
    "equity": ("BS", "자본총계"),
}


def parse_amount(series: pd.Series) -> pd.Series:
    return pd.to_numeric(
        series.fillna("").astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )


def unique_amount(
    frame: pd.DataFrame,
    company_id: str,
    period_label: str,
    statement: str,
    account_name: str,
    amount_field: str,
) -> float:
    selected = frame[
        (frame["company_id"] == company_id)
        & (frame["period_label"] == period_label)
        & (frame["sj_div"] == statement)
        & (frame["account_nm"] == account_name)
    ]
    values = parse_amount(selected[amount_field]).dropna().unique()
    if len(values) != 1:
        raise ValueError(
            f"Expected one {amount_field} for {company_id} {period_label} "
            f"{account_name}; found {values.tolist()}"
        )
    return float(values[0])


def cash_amount(accounts: pd.DataFrame, company_id: str) -> float:
    selected = accounts[
        (accounts["company_id"] == company_id)
        & (accounts["period_label"] == "Q1_2026")
        & (accounts["sj_div"] == "BS")
        & (accounts["account_id"] == "ifrs-full_CashAndCashEquivalents")
        & (accounts["account_detail"] == "-")
    ]
    values = parse_amount(selected["thstrm_amount"]).dropna().unique()
    if len(values) != 1:
        raise ValueError(
            f"Expected one consolidated cash balance for {company_id}; "
            f"found {values.tolist()}"
        )
    return float(values[0])


def safe_growth(current: float, prior: float) -> float:
    if prior == 0:
        return float("nan")
    return (current - prior) / abs(prior) * 100


def profit_direction(current: float, prior: float) -> str:
    if prior < 0 <= current:
        return "Turned profitable"
    if prior >= 0 > current:
        return "Turned to loss"
    if current >= prior:
        return "Improved"
    return "Deteriorated"


def build_summary(dataset_dir: Path) -> pd.DataFrame:
    highlights = pd.read_csv(
        dataset_dir / "financial_highlights_long.csv", dtype=str
    )
    accounts = pd.read_csv(dataset_dir / "financial_accounts.csv", dtype=str)
    companies = pd.read_csv(dataset_dir / "companies.csv", dtype=str)
    rows: list[dict[str, Any]] = []
    for company in companies.to_dict("records"):
        company_id = company["company_id"]
        row: dict[str, Any] = {**company}
        for metric, (statement, account_name) in METRICS.items():
            row[f"fy2025_{metric}_krw"] = unique_amount(
                highlights,
                company_id,
                "FY2025",
                statement,
                account_name,
                "thstrm_amount",
            )
            row[f"q1_2026_{metric}_krw"] = unique_amount(
                highlights,
                company_id,
                "Q1_2026",
                statement,
                account_name,
                "thstrm_amount",
            )
            if statement == "IS":
                row[f"q1_2025_{metric}_krw"] = unique_amount(
                    highlights,
                    company_id,
                    "Q1_2026",
                    statement,
                    account_name,
                    "frmtrm_amount",
                )
        row["q1_2026_cash_krw"] = cash_amount(accounts, company_id)
        row["fy2025_operating_margin_pct"] = (
            row["fy2025_operating_profit_krw"] / row["fy2025_revenue_krw"] * 100
        )
        row["q1_2025_operating_margin_pct"] = (
            row["q1_2025_operating_profit_krw"]
            / row["q1_2025_revenue_krw"]
            * 100
        )
        row["q1_2026_operating_margin_pct"] = (
            row["q1_2026_operating_profit_krw"]
            / row["q1_2026_revenue_krw"]
            * 100
        )
        row["q1_revenue_yoy_pct"] = safe_growth(
            row["q1_2026_revenue_krw"], row["q1_2025_revenue_krw"]
        )
        row["q1_operating_profit_change_krw"] = (
            row["q1_2026_operating_profit_krw"]
            - row["q1_2025_operating_profit_krw"]
        )
        row["q1_operating_profit_change_pct"] = safe_growth(
            row["q1_2026_operating_profit_krw"],
            row["q1_2025_operating_profit_krw"],
        )
        row["q1_profit_direction"] = profit_direction(
            row["q1_2026_operating_profit_krw"],
            row["q1_2025_operating_profit_krw"],
        )
        row["q1_2026_debt_to_equity_pct"] = (
            row["q1_2026_liabilities_krw"] / row["q1_2026_equity_krw"] * 100
        )
        row["q1_2026_equity_ratio_pct"] = (
            row["q1_2026_equity_krw"] / row["q1_2026_assets_krw"] * 100
        )
        row["q1_2026_cash_to_assets_pct"] = (
            row["q1_2026_cash_krw"] / row["q1_2026_assets_krw"] * 100
        )
        rows.append(row)
    summary = pd.DataFrame(rows).sort_values("company_name").reset_index(drop=True)
    summary.to_csv(dataset_dir / "financial_summary.csv", index=False)
    return summary


def style() -> None:
    sns.set_theme(style="whitegrid", context="talk")
    plt.rcParams.update(
        {
            "figure.facecolor": "white",
            "axes.facecolor": "white",
            "axes.titleweight": "bold",
            "axes.titlepad": 16,
            "font.family": "DejaVu Sans",
        }
    )


def company_palette(summary: pd.DataFrame) -> dict[str, Any]:
    colors = sns.color_palette("husl", len(summary))
    return dict(zip(sorted(summary["company_name"]), colors))


def save_chart(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    plt.savefig(path, dpi=180, bbox_inches="tight", facecolor="white")
    plt.close()


def build_charts(summary: pd.DataFrame, chart_dir: Path) -> list[Path]:
    style()
    palette = company_palette(summary)
    outputs: list[Path] = []

    ordered = summary.sort_values("fy2025_revenue_krw")
    fig, axes = plt.subplots(
        1,
        2,
        figsize=(16, 8),
        sharey=True,
        gridspec_kw={"width_ratios": [1.4, 1]},
    )
    axes[0].barh(
        ordered["company_name"],
        ordered["fy2025_revenue_krw"] / 1e12,
        color=[palette[name] for name in ordered["company_name"]],
    )
    axes[0].set(title="FY2025 Revenue", xlabel="KRW trillion", ylabel="")
    for index, value in enumerate(ordered["fy2025_revenue_krw"] / 1e12):
        axes[0].text(value + 0.03, index, f"{value:.2f}", va="center", fontsize=11)
    axes[1].barh(
        ordered["company_name"],
        ordered["fy2025_operating_margin_pct"],
        color=[palette[name] for name in ordered["company_name"]],
    )
    axes[1].axvline(0, color="#333333", linewidth=1)
    axes[1].set(title="FY2025 Operating Margin", xlabel="Percent", ylabel="")
    axes[1].tick_params(axis="y", labelleft=False)
    fig.subplots_adjust(wspace=0.12)
    fig.suptitle("Korean Game Companies: Scale and Profitability", fontsize=20, fontweight="bold")
    fig.text(0.5, -0.01, "Source: Open DART consolidated financial statements", ha="center", fontsize=10)
    output = chart_dir / "01_fy2025_scale_and_margin.png"
    save_chart(output)
    outputs.append(output)

    ordered = summary.sort_values("q1_revenue_yoy_pct")
    colors = ["#2A9D8F" if value >= 0 else "#E76F51" for value in ordered["q1_revenue_yoy_pct"]]
    plt.figure(figsize=(12, 7))
    plt.barh(ordered["company_name"], ordered["q1_revenue_yoy_pct"], color=colors)
    plt.axvline(0, color="#333333", linewidth=1)
    plt.title("Q1 2026 Revenue Growth vs Q1 2025")
    plt.xlabel("Year-over-year growth (%)")
    plt.ylabel("")
    plt.xlim(
        min(-60, float(ordered["q1_revenue_yoy_pct"].min()) - 10),
        float(ordered["q1_revenue_yoy_pct"].max()) * 1.06,
    )
    for index, value in enumerate(ordered["q1_revenue_yoy_pct"]):
        if value >= 0:
            plt.text(value + 4, index, f"{value:.1f}%", va="center", ha="left", fontsize=11)
        else:
            plt.text(value - 2, index, f"{value:.1f}%", va="center", ha="right", fontsize=11)
    output = chart_dir / "02_q1_revenue_growth.png"
    save_chart(output)
    outputs.append(output)

    ordered = summary.sort_values("q1_2026_operating_margin_pct")
    positions = np.arange(len(ordered))
    width = 0.38
    plt.figure(figsize=(14, 8))
    plt.barh(positions - width / 2, ordered["q1_2025_operating_margin_pct"], height=width, label="Q1 2025", color="#A8DADC")
    plt.barh(positions + width / 2, ordered["q1_2026_operating_margin_pct"], height=width, label="Q1 2026", color="#457B9D")
    plt.yticks(positions, ordered["company_name"])
    plt.axvline(0, color="#333333", linewidth=1)
    plt.title("Operating Margin: Q1 2025 vs Q1 2026")
    plt.xlabel("Operating margin (%)")
    plt.ylabel("")
    plt.legend(frameon=False)
    output = chart_dir / "03_q1_operating_margin.png"
    save_chart(output)
    outputs.append(output)

    plt.figure(figsize=(12, 8))
    sizes = 250 + summary["q1_2026_assets_krw"] / summary["q1_2026_assets_krw"].max() * 1500
    stability_offsets = {
        "Pearl Abyss": (8, 17),
        "Netmarble": (8, 12),
        "Com2uS": (12, -22),
        "Kakao Games": (-98, 18),
        "WEMADE": (10, -14),
    }
    for _, row in summary.iterrows():
        plt.scatter(
            row["q1_2026_debt_to_equity_pct"],
            row["q1_2026_cash_to_assets_pct"],
            s=sizes.loc[row.name],
            color=palette[row["company_name"]],
            alpha=0.8,
            edgecolor="white",
            linewidth=1.5,
        )
        plt.annotate(
            row["company_name"],
            (row["q1_2026_debt_to_equity_pct"], row["q1_2026_cash_to_assets_pct"]),
            xytext=stability_offsets.get(row["company_name"], (7, 5)),
            textcoords="offset points",
            fontsize=10,
        )
    plt.title("Balance-Sheet Position at Q1 2026")
    plt.xlabel("Debt-to-equity (%)")
    plt.ylabel("Cash and equivalents / assets (%)")
    plt.figtext(0.5, -0.01, "Bubble size represents total assets", ha="center", fontsize=10)
    output = chart_dir / "04_balance_sheet_position.png"
    save_chart(output)
    outputs.append(output)

    plt.figure(figsize=(13, 8))
    sizes = 250 + summary["q1_2026_revenue_krw"] / summary["q1_2026_revenue_krw"].max() * 1500
    plt.axhline(0, color="#777777", linewidth=1)
    plt.axvline(0, color="#777777", linewidth=1)
    growth_offsets = {
        "Pearl Abyss": (-78, 10),
        "Netmarble": (8, 14),
        "Com2uS": (-32, 9),
        "WEMADE": (12, -28),
        "Devsisters": (8, 14),
        "Kakao Games": (8, -18),
    }
    for _, row in summary.iterrows():
        plt.scatter(
            row["q1_revenue_yoy_pct"],
            row["q1_2026_operating_margin_pct"],
            s=sizes.loc[row.name],
            color=palette[row["company_name"]],
            alpha=0.82,
            edgecolor="white",
            linewidth=1.5,
        )
        plt.annotate(
            row["company_name"],
            (row["q1_revenue_yoy_pct"], row["q1_2026_operating_margin_pct"]),
            xytext=growth_offsets.get(row["company_name"], (7, 5)),
            textcoords="offset points",
            fontsize=10,
        )
    plt.title("Q1 2026 Growth–Profitability Map")
    plt.xlabel("Revenue growth YoY (%)")
    plt.ylabel("Operating margin (%)")
    plt.figtext(0.5, -0.01, "Bubble size represents Q1 2026 revenue", ha="center", fontsize=10)
    output = chart_dir / "05_growth_profitability_map.png"
    save_chart(output)
    outputs.append(output)
    return outputs


def build_findings(summary: pd.DataFrame, dataset_dir: Path) -> dict[str, Any]:
    largest = summary.loc[summary["fy2025_revenue_krw"].idxmax()]
    margin = summary.loc[summary["fy2025_operating_margin_pct"].idxmax()]
    growth = summary.loc[summary["q1_revenue_yoy_pct"].idxmax()]
    cash = summary.loc[summary["q1_2026_cash_to_assets_pct"].idxmax()]
    findings = {
        "largest_fy2025_revenue_company": largest["company_name"],
        "largest_fy2025_revenue_krw": int(largest["fy2025_revenue_krw"]),
        "highest_fy2025_operating_margin_company": margin["company_name"],
        "highest_fy2025_operating_margin_pct": round(float(margin["fy2025_operating_margin_pct"]), 2),
        "fastest_q1_revenue_growth_company": growth["company_name"],
        "fastest_q1_revenue_growth_pct": round(float(growth["q1_revenue_yoy_pct"]), 2),
        "highest_cash_to_assets_company": cash["company_name"],
        "highest_cash_to_assets_pct": round(float(cash["q1_2026_cash_to_assets_pct"]), 2),
        "profit_direction_counts": summary["q1_profit_direction"].value_counts().to_dict(),
    }
    (dataset_dir / "key_findings.json").write_text(
        json.dumps(findings, ensure_ascii=False, indent=2) + "\n", encoding="utf-8"
    )
    return findings


FIELD_DESCRIPTIONS = {
    "company_id": "Stable project identifier for the company.",
    "company_name": "English display name used in the analysis.",
    "corp_code": "Eight-digit Open DART corporation code.",
    "stock_code": "Six-digit Korea Exchange stock code.",
    "market": "Primary Korea Exchange market segment.",
    "period_label": "Analysis period label: FY2025 or Q1_2026.",
    "business_year": "Business year supplied to the Open DART API.",
    "report_code": "Open DART report code (11011 annual, 11013 first quarter).",
    "report_name": "English description of the report type.",
    "rcept_no": "Fourteen-digit Open DART filing receipt number.",
    "fs_div": "Financial statement scope; CFS means consolidated.",
    "fs_nm": "Financial statement scope name returned by Open DART.",
    "sj_div": "Statement code such as BS, IS, CIS, CF, or SCE.",
    "sj_nm": "Financial statement name returned by Open DART.",
    "account_id": "XBRL/DART account identifier.",
    "account_nm": "Account name returned by Open DART.",
    "account_detail": "Dimensional account detail returned by Open DART.",
    "currency": "Currency reported by Open DART.",
    "source_endpoint": "Open DART endpoint used to obtain the row.",
    "corp_cls": "DART corporation class: Y, K, N, or E.",
    "corp_name": "Company name returned in the disclosure search.",
    "flr_nm": "Name of the filing submitter.",
    "rcept_dt": "Filing receipt date in YYYYMMDD format.",
    "report_nm": "Disclosure report title.",
    "rm": "Combined Open DART filing remark flags.",
    "viewer_url": "Public DART filing-viewer URL.",
    "q1_profit_direction": "Categorical operating-profit movement versus Q1 2025.",
}


def describe_column(name: str) -> tuple[str, str, str]:
    if name in FIELD_DESCRIPTIONS:
        return FIELD_DESCRIPTIONS[name], "", "Direct field unless noted."
    if name.endswith("_krw"):
        return (
            name.replace("_", " ").capitalize() + ".",
            "KRW",
            "Derived from the cited consolidated Open DART statement.",
        )
    if name.endswith("_pct"):
        return (
            name.replace("_", " ").capitalize() + ".",
            "percent",
            "Calculated from published KRW values; see README methodology.",
        )
    if name.endswith("_amount"):
        return "Amount returned by Open DART.", "KRW", "Direct Open DART field."
    if name.endswith("_dt"):
        return "Period or date description returned by Open DART.", "", "Direct Open DART field."
    if name.endswith("_nm"):
        return "Period or field name returned by Open DART.", "", "Direct Open DART field."
    return f"Open DART or derived field: {name}.", "", "See source CSV and README."


def build_data_dictionary(dataset_dir: Path) -> None:
    rows: list[dict[str, str]] = []
    for path in sorted(dataset_dir.glob("*.csv")):
        if path.name == "data_dictionary.csv":
            continue
        with path.open(encoding="utf-8", newline="") as handle:
            fieldnames = next(csv.reader(handle))
        for name in fieldnames:
            description, unit, derivation = describe_column(name)
            rows.append(
                {
                    "file": path.name,
                    "column": name,
                    "description": description,
                    "unit": unit,
                    "derivation": derivation,
                }
            )
    pd.DataFrame(rows).to_csv(dataset_dir / "data_dictionary.csv", index=False)


def notebook_cells() -> list[Any]:
    return [
        nbf.v4.new_markdown_cell(
            """# Korean Game Companies: Financial Health in 2025–2026

This notebook compares eight Korean listed game publishers using consolidated financial statements from the Financial Supervisory Service's Open DART API.

**Questions**

1. Which companies led FY2025 in scale and operating profitability?
2. How did Q1 2026 revenue and operating margins change year over year?
3. How do leverage and cash buffers differ across the peer group?
4. Which companies occupy the strongest growth–profitability quadrant?

All monetary values are KRW. This is descriptive analysis, not investment advice."""
        ),
        nbf.v4.new_code_cell(
            """from pathlib import Path
import json
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

sns.set_theme(style="whitegrid", context="talk")
plt.rcParams.update({"figure.facecolor": "white", "axes.facecolor": "white", "axes.titleweight": "bold"})

candidates = [
    Path("/kaggle/input/korean-game-companies-open-dart-2025-2026"),
    Path("../dataset"),
    Path("kaggle/dataset"),
]
DATA_DIR = next((path for path in candidates if (path / "financial_summary.csv").exists()), None)
if DATA_DIR is None:
    raise FileNotFoundError("Could not locate the Kaggle dataset directory")

summary = pd.read_csv(DATA_DIR / "financial_summary.csv", dtype={"corp_code": str, "stock_code": str})
highlights = pd.read_csv(DATA_DIR / "financial_highlights_long.csv", dtype=str)
disclosures = pd.read_csv(DATA_DIR / "disclosures_2026.csv", dtype=str)
print(f"Data directory: {DATA_DIR}")
print(f"Companies: {len(summary)} | Highlight rows: {len(highlights):,} | 2026 filings: {len(disclosures):,}")
summary[["company_name", "market", "stock_code"]]"""
        ),
        nbf.v4.new_markdown_cell(
            """## Method and comparability

- Consolidated statements (`CFS`) are used for all eight companies.
- FY2025 values come from annual reports (`11011`).
- Q1 comparisons use first-quarter reports (`11013`) and their disclosed Q1 2025 comparatives.
- Annual and quarterly absolute amounts are never compared directly.
- Revenue growth uses `(Q1 2026 − Q1 2025) / Q1 2025`.
- A percentage change in operating profit can be misleading around zero or when its sign changes, so the notebook emphasizes margin and a turnaround/loss flag instead.
- Corrected filings are represented by the receipt numbers returned by the financial-statement API."""
        ),
        nbf.v4.new_code_cell(
            """money_cols = [column for column in summary if column.endswith("_krw")]
assert summary["company_id"].nunique() == 8
assert summary[money_cols].notna().all().all()
assert (summary["currency"] == "KRW").all() if "currency" in summary else True

display_cols = [
    "company_name", "fy2025_revenue_krw", "fy2025_operating_margin_pct",
    "q1_revenue_yoy_pct", "q1_2026_operating_margin_pct", "q1_profit_direction",
    "q1_2026_debt_to_equity_pct", "q1_2026_cash_to_assets_pct",
]
summary[display_cols].sort_values("fy2025_revenue_krw", ascending=False).style.format({
    "fy2025_revenue_krw": "{:,.0f}",
    "fy2025_operating_margin_pct": "{:.1f}%",
    "q1_revenue_yoy_pct": "{:.1f}%",
    "q1_2026_operating_margin_pct": "{:.1f}%",
    "q1_2026_debt_to_equity_pct": "{:.1f}%",
    "q1_2026_cash_to_assets_pct": "{:.1f}%",
})"""
        ),
        nbf.v4.new_markdown_cell("## 1. FY2025 scale and profitability"),
        nbf.v4.new_code_cell(
            """ordered = summary.sort_values("fy2025_revenue_krw")
palette = dict(zip(sorted(summary.company_name), sns.color_palette("husl", len(summary))))
fig, axes = plt.subplots(1, 2, figsize=(16, 8), sharey=True, gridspec_kw={"width_ratios": [1.4, 1]})
axes[0].barh(ordered.company_name, ordered.fy2025_revenue_krw / 1e12, color=[palette[x] for x in ordered.company_name])
axes[0].set(title="FY2025 Revenue", xlabel="KRW trillion", ylabel="")
axes[1].barh(ordered.company_name, ordered.fy2025_operating_margin_pct, color=[palette[x] for x in ordered.company_name])
axes[1].axvline(0, color="#333333", linewidth=1)
axes[1].set(title="FY2025 Operating Margin", xlabel="Percent", ylabel="")
axes[1].tick_params(axis="y", labelleft=False)
fig.suptitle("Korean Game Companies: Scale and Profitability", fontsize=20, fontweight="bold")
fig.subplots_adjust(wspace=.12)
plt.tight_layout()
plt.show()"""
        ),
        nbf.v4.new_markdown_cell("## 2. Q1 2026 revenue growth"),
        nbf.v4.new_code_cell(
            """ordered = summary.sort_values("q1_revenue_yoy_pct")
colors = ["#2A9D8F" if value >= 0 else "#E76F51" for value in ordered.q1_revenue_yoy_pct]
plt.figure(figsize=(12, 7))
plt.barh(ordered.company_name, ordered.q1_revenue_yoy_pct, color=colors)
plt.axvline(0, color="#333333", linewidth=1)
plt.title("Q1 2026 Revenue Growth vs Q1 2025")
plt.xlabel("Year-over-year growth (%)")
plt.ylabel("")
plt.show()"""
        ),
        nbf.v4.new_markdown_cell("## 3. Operating-margin movement"),
        nbf.v4.new_code_cell(
            """ordered = summary.sort_values("q1_2026_operating_margin_pct")
positions = np.arange(len(ordered)); width = 0.38
plt.figure(figsize=(14, 8))
plt.barh(positions - width/2, ordered.q1_2025_operating_margin_pct, height=width, label="Q1 2025", color="#A8DADC")
plt.barh(positions + width/2, ordered.q1_2026_operating_margin_pct, height=width, label="Q1 2026", color="#457B9D")
plt.yticks(positions, ordered.company_name)
plt.axvline(0, color="#333333", linewidth=1)
plt.title("Operating Margin: Q1 2025 vs Q1 2026")
plt.xlabel("Operating margin (%)"); plt.ylabel(""); plt.legend(frameon=False)
plt.show()
summary[["company_name", "q1_profit_direction"]].sort_values("company_name")"""
        ),
        nbf.v4.new_markdown_cell("## 4. Balance-sheet position"),
        nbf.v4.new_code_cell(
            """plt.figure(figsize=(12, 8))
sizes = 250 + summary.q1_2026_assets_krw / summary.q1_2026_assets_krw.max() * 1500
offsets = {"Pearl Abyss": (8,17), "Netmarble": (8,12), "Com2uS": (12,-22), "Kakao Games": (-98,18), "WEMADE": (10,-14)}
for idx, row in summary.iterrows():
    plt.scatter(row.q1_2026_debt_to_equity_pct, row.q1_2026_cash_to_assets_pct, s=sizes.loc[idx], color=palette[row.company_name], alpha=.8, edgecolor="white")
    plt.annotate(row.company_name, (row.q1_2026_debt_to_equity_pct, row.q1_2026_cash_to_assets_pct), xytext=offsets.get(row.company_name, (7,5)), textcoords="offset points", fontsize=10)
plt.title("Balance-Sheet Position at Q1 2026")
plt.xlabel("Debt-to-equity (%)"); plt.ylabel("Cash and equivalents / assets (%)")
plt.show()"""
        ),
        nbf.v4.new_markdown_cell("## 5. Growth–profitability map"),
        nbf.v4.new_code_cell(
            """plt.figure(figsize=(13, 8))
sizes = 250 + summary.q1_2026_revenue_krw / summary.q1_2026_revenue_krw.max() * 1500
plt.axhline(0, color="#777", linewidth=1); plt.axvline(0, color="#777", linewidth=1)
offsets = {"Pearl Abyss": (-78,10), "Netmarble": (8,14), "Com2uS": (-32,9), "WEMADE": (12,-28), "Devsisters": (8,14), "Kakao Games": (8,-18)}
for idx, row in summary.iterrows():
    plt.scatter(row.q1_revenue_yoy_pct, row.q1_2026_operating_margin_pct, s=sizes.loc[idx], color=palette[row.company_name], alpha=.82, edgecolor="white")
    plt.annotate(row.company_name, (row.q1_revenue_yoy_pct, row.q1_2026_operating_margin_pct), xytext=offsets.get(row.company_name, (7,5)), textcoords="offset points", fontsize=10)
plt.title("Q1 2026 Growth–Profitability Map")
plt.xlabel("Revenue growth YoY (%)"); plt.ylabel("Operating margin (%)")
plt.show()"""
        ),
        nbf.v4.new_markdown_cell("## Data-driven takeaways"),
        nbf.v4.new_code_cell(
            """largest = summary.loc[summary.fy2025_revenue_krw.idxmax()]
best_margin = summary.loc[summary.fy2025_operating_margin_pct.idxmax()]
fastest = summary.loc[summary.q1_revenue_yoy_pct.idxmax()]
cashiest = summary.loc[summary.q1_2026_cash_to_assets_pct.idxmax()]
print(f"• Largest FY2025 revenue: {largest.company_name} (KRW {largest.fy2025_revenue_krw/1e12:.2f}tn)")
print(f"• Highest FY2025 operating margin: {best_margin.company_name} ({best_margin.fy2025_operating_margin_pct:.1f}%)")
print(f"• Fastest Q1 revenue growth: {fastest.company_name} ({fastest.q1_revenue_yoy_pct:.1f}% YoY)")
print(f"• Highest Q1 cash/assets ratio: {cashiest.company_name} ({cashiest.q1_2026_cash_to_assets_pct:.1f}%)")
print("• Operating-profit direction:", summary.q1_profit_direction.value_counts().to_dict())"""
        ),
        nbf.v4.new_markdown_cell(
            """## Limitations

- This is a small peer set of eight listed Korean game publishers, not the entire industry.
- Accounting classifications and consolidation scopes can differ by company.
- Q1 results can be seasonal and should not be annualized mechanically.
- Extreme growth rates can reflect a low comparison base, acquisitions, disposals, or major product launches; the filings should be read before assigning causality.
- Open DART republishes filer-submitted data and does not guarantee its accuracy or completeness.

**Source:** Financial Supervisory Service, Open DART. Analysis date: 2026-07-26. Not investment advice."""
        ),
    ]


def build_notebook(path: Path) -> None:
    notebook = nbf.v4.new_notebook(
        cells=notebook_cells(),
        metadata={
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python", "version": "3.11"},
        },
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    nbf.write(notebook, path)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Build Kaggle analysis artifacts.")
    parser.add_argument("--dataset-dir", type=Path, default=DEFAULT_DATASET_DIR)
    parser.add_argument("--chart-dir", type=Path, default=DEFAULT_CHART_DIR)
    parser.add_argument("--notebook", type=Path, default=DEFAULT_NOTEBOOK)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary = build_summary(args.dataset_dir)
    outputs = build_charts(summary, args.chart_dir)
    findings = build_findings(summary, args.dataset_dir)
    build_data_dictionary(args.dataset_dir)
    build_notebook(args.notebook)
    print(f"Summary rows: {len(summary)}")
    print(f"Charts: {len(outputs)}")
    print(f"Notebook: {args.notebook.resolve()}")
    print(json.dumps(findings, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
