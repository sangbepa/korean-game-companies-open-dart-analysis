#!/usr/bin/env python3
"""Build a local DuckDB database for multidimensional financial analysis.

The generated sample figures are deterministic for a given seed and represent
whole USD amounts. The script is intentionally idempotent: each run rebuilds
only the three tables owned by this model inside one transaction.
"""

from __future__ import annotations

import argparse
import random
from pathlib import Path
from typing import Any, Iterable, Sequence

import duckdb


DATABASE_FILENAME = "financial_analytics.duckdb"
DEFAULT_SEED = 20260726

# Refuse paths that clearly point to common cloud-sync locations. A company may
# use other sync products, so callers should still choose a known local folder.
CLOUD_PATH_MARKERS = (
    "icloud",
    "mobile documents",
    "cloudstorage",
    "onedrive",
    "dropbox",
    "google drive",
    "googledrive",
    "box sync",
)

REGIONS: list[tuple[int, str]] = [
    (1, "APAC"),
    (2, "EMEA"),
    (3, "Americas"),
]

CORPS: list[tuple[int, str, int]] = [
    (1, "CorpA", 1),
    (2, "CorpB", 1),
    (3, "CorpC", 1),
    (4, "CorpD", 2),
    (5, "CorpE", 2),
    (6, "CorpF", 2),
    (7, "CorpG", 3),
    (8, "CorpH", 3),
    (9, "CorpI", 3),
]

YEARS = (2024, 2025, 2026)


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments."""
    default_database = Path(__file__).resolve().with_name(DATABASE_FILENAME)
    parser = argparse.ArgumentParser(
        description="Create and verify the financial analytics DuckDB database."
    )
    parser.add_argument(
        "--database",
        type=Path,
        default=default_database,
        help=f"Local database path (default: {default_database})",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help=f"Random seed for repeatable sample data (default: {DEFAULT_SEED})",
    )
    return parser.parse_args()


def validate_local_database_path(database_path: Path) -> Path:
    """Return a normalized path after rejecting recognizable cloud locations."""
    expanded_path = database_path.expanduser()
    resolved_path = expanded_path.resolve(strict=False)
    normalized_path = str(resolved_path).casefold()

    matched_markers = [
        marker for marker in CLOUD_PATH_MARKERS if marker in normalized_path
    ]
    if matched_markers:
        markers = ", ".join(matched_markers)
        raise ValueError(
            f"Refusing cloud-synced database path {resolved_path!s} "
            f"(matched: {markers}). Choose a known local directory."
        )

    if expanded_path.is_symlink():
        raise ValueError(
            f"Refusing symlinked database path {expanded_path!s}; "
            "use an explicit path in a known local directory."
        )

    if resolved_path.suffix.casefold() != ".duckdb":
        raise ValueError("The database path must use the .duckdb extension.")

    resolved_path.parent.mkdir(parents=True, exist_ok=True)
    return resolved_path


def generate_financial_rows(seed: int) -> list[tuple[int, ...]]:
    """Generate coherent financial statements for every corporation and year."""
    rng = random.Random(seed)
    rows: list[tuple[int, ...]] = []

    for corp_id, _corp_name, region_id in CORPS:
        # Company-specific scale and growth assumptions keep year-over-year
        # patterns plausible while preserving variation across corporations.
        revenue = rng.randint(320_000_000, 1_450_000_000)
        target_growth = rng.uniform(0.035, 0.125)

        for year_index, year in enumerate(YEARS):
            if year_index:
                annual_growth = target_growth + rng.uniform(-0.025, 0.025)
                revenue = round(revenue * (1.0 + annual_growth))

            gross_margin = rng.uniform(0.29, 0.49)
            gross_profit = round(revenue * gross_margin)
            cost_of_goods_sold = revenue - gross_profit

            operating_expenses = round(gross_profit * rng.uniform(0.54, 0.80))
            operating_income = gross_profit - operating_expenses
            net_income = round(operating_income * rng.uniform(0.61, 0.82))

            total_assets = round(revenue * rng.uniform(0.95, 1.55))
            total_liabilities = round(total_assets * rng.uniform(0.36, 0.67))
            equity = total_assets - total_liabilities

            rows.append(
                (
                    region_id,
                    corp_id,
                    year,
                    revenue,
                    cost_of_goods_sold,
                    gross_profit,
                    operating_expenses,
                    operating_income,
                    net_income,
                    total_assets,
                    total_liabilities,
                    equity,
                )
            )

    return rows


def rebuild_schema(connection: duckdb.DuckDBPyConnection, seed: int) -> None:
    """Create the star-schema tables and their integrity constraints."""
    connection.execute("BEGIN TRANSACTION")
    try:
        connection.execute("DROP TABLE IF EXISTS fact_financials")
        connection.execute("DROP TABLE IF EXISTS dim_corp")
        connection.execute("DROP TABLE IF EXISTS dim_region")

        connection.execute(
            """
            CREATE TABLE dim_region (
                region_id INTEGER PRIMARY KEY,
                region_name VARCHAR NOT NULL UNIQUE
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE dim_corp (
                corp_id INTEGER PRIMARY KEY,
                corp_name VARCHAR NOT NULL UNIQUE,
                region_id INTEGER NOT NULL,
                UNIQUE (corp_id, region_id),
                FOREIGN KEY (region_id) REFERENCES dim_region (region_id)
            )
            """
        )

        connection.execute(
            """
            CREATE TABLE fact_financials (
                region_id INTEGER NOT NULL,
                corp_id INTEGER NOT NULL,
                year INTEGER NOT NULL,
                Revenue BIGINT NOT NULL,
                "Cost of Goods Sold" BIGINT NOT NULL,
                "Gross Profit" BIGINT NOT NULL,
                "Operating Expenses" BIGINT NOT NULL,
                "Operating Income" BIGINT NOT NULL,
                "Net Income" BIGINT NOT NULL,
                "Total Assets" BIGINT NOT NULL,
                "Total Liabilities" BIGINT NOT NULL,
                Equity BIGINT NOT NULL,
                PRIMARY KEY (region_id, corp_id, year),
                FOREIGN KEY (region_id) REFERENCES dim_region (region_id),
                FOREIGN KEY (corp_id, region_id)
                    REFERENCES dim_corp (corp_id, region_id),
                CHECK (year BETWEEN 1900 AND 2200),
                CHECK (Revenue >= 0),
                CHECK ("Cost of Goods Sold" >= 0),
                CHECK ("Gross Profit" = Revenue - "Cost of Goods Sold"),
                CHECK ("Operating Expenses" >= 0),
                CHECK ("Operating Income" = "Gross Profit" - "Operating Expenses"),
                CHECK ("Net Income" BETWEEN 0 AND "Operating Income"),
                CHECK ("Total Assets" >= 0),
                CHECK ("Total Liabilities" >= 0),
                CHECK (Equity >= 0),
                CHECK ("Total Assets" = "Total Liabilities" + Equity)
            )
            """
        )

        connection.executemany(
            "INSERT INTO dim_region (region_id, region_name) VALUES (?, ?)",
            REGIONS,
        )
        connection.executemany(
            "INSERT INTO dim_corp (corp_id, corp_name, region_id) VALUES (?, ?, ?)",
            CORPS,
        )
        connection.executemany(
            """
            INSERT INTO fact_financials (
                region_id,
                corp_id,
                year,
                Revenue,
                "Cost of Goods Sold",
                "Gross Profit",
                "Operating Expenses",
                "Operating Income",
                "Net Income",
                "Total Assets",
                "Total Liabilities",
                Equity
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            generate_financial_rows(seed),
        )
        connection.execute("COMMIT")
    except Exception:
        connection.execute("ROLLBACK")
        raise


def format_value(column_name: str, value: Any) -> str:
    """Format a query value for compact console output."""
    if value is None:
        return "NULL"
    if isinstance(value, int):
        identifier_columns = {"cid", "notnull", "pk", "row_count", "year"}
        if column_name.casefold() in identifier_columns or column_name.endswith("_id"):
            return str(value)
        return f"{value:,}"
    return str(value)


def print_rows(
    title: str,
    column_names: Sequence[str],
    rows: Iterable[Sequence[Any]],
) -> None:
    """Print query results without requiring pandas or another dependency."""
    formatted_rows = [
        [format_value(name, value) for name, value in zip(column_names, row)]
        for row in rows
    ]
    widths = [len(name) for name in column_names]
    for row in formatted_rows:
        widths = [max(width, len(value)) for width, value in zip(widths, row)]

    print(f"\n{title}")
    print(" | ".join(name.ljust(width) for name, width in zip(column_names, widths)))
    print("-+-".join("-" * width for width in widths))
    for row in formatted_rows:
        print(" | ".join(value.ljust(width) for value, width in zip(row, widths)))


def print_query(
    connection: duckdb.DuckDBPyConnection,
    title: str,
    query: str,
    parameters: Sequence[Any] | None = None,
) -> None:
    """Execute a query and render its result as a console table."""
    cursor = connection.execute(query, parameters or [])
    column_names = [description[0] for description in cursor.description]
    print_rows(title, column_names, cursor.fetchall())


def verify_and_report(connection: duckdb.DuckDBPyConnection) -> None:
    """Print schemas, integrity checks, and representative analyses."""
    for table_name in ("dim_region", "dim_corp", "fact_financials"):
        print_query(
            connection,
            f"Schema: {table_name}",
            f"PRAGMA table_info('{table_name}')",
        )

    print_query(
        connection,
        "Verification: row counts",
        """
        SELECT 'dim_region' AS table_name, COUNT(*) AS row_count FROM dim_region
        UNION ALL
        SELECT 'dim_corp', COUNT(*) FROM dim_corp
        UNION ALL
        SELECT 'fact_financials', COUNT(*) FROM fact_financials
        ORDER BY table_name
        """,
    )

    print_query(
        connection,
        "Verification: accounting and hierarchy exceptions (all should be 0)",
        """
        SELECT
            SUM(CASE WHEN f."Gross Profit" <> f.Revenue - f."Cost of Goods Sold"
                     THEN 1 ELSE 0 END) AS income_statement_exceptions,
            SUM(CASE WHEN f."Operating Income" <>
                              f."Gross Profit" - f."Operating Expenses"
                     THEN 1 ELSE 0 END) AS operating_income_exceptions,
            SUM(CASE WHEN f."Total Assets" <>
                              f."Total Liabilities" + f.Equity
                     THEN 1 ELSE 0 END) AS balance_sheet_exceptions,
            SUM(CASE WHEN f.region_id <> c.region_id
                     THEN 1 ELSE 0 END) AS hierarchy_exceptions
        FROM fact_financials AS f
        INNER JOIN dim_corp AS c ON c.corp_id = f.corp_id
        """,
    )

    print_query(
        connection,
        "Gross Margin by Corp in APAC for 2026",
        """
        SELECT
            c.corp_name AS Corp,
            f.year AS Year,
            f.Revenue,
            f."Gross Profit",
            ROUND(100.0 * f."Gross Profit" / NULLIF(f.Revenue, 0), 2)
                AS "Gross Margin %"
        FROM fact_financials AS f
        INNER JOIN dim_corp AS c ON c.corp_id = f.corp_id
        INNER JOIN dim_region AS r ON r.region_id = f.region_id
        WHERE r.region_name = ? AND f.year = ?
        ORDER BY c.corp_name
        """,
        ["APAC", 2026],
    )

    print_query(
        connection,
        "YoY Revenue Growth by Corp",
        """
        WITH revenue_history AS (
            SELECT
                r.region_name AS Region,
                c.corp_name AS Corp,
                f.year AS Year,
                f.Revenue,
                LAG(f.Revenue) OVER (
                    PARTITION BY f.corp_id ORDER BY f.year
                ) AS prior_year_revenue
            FROM fact_financials AS f
            INNER JOIN dim_corp AS c ON c.corp_id = f.corp_id
            INNER JOIN dim_region AS r ON r.region_id = f.region_id
        )
        SELECT
            Region,
            Corp,
            Year,
            Revenue,
            prior_year_revenue AS "Prior Year Revenue",
            ROUND(
                100.0 * (Revenue - prior_year_revenue)
                / NULLIF(prior_year_revenue, 0),
                2
            ) AS "YoY Growth %"
        FROM revenue_history
        WHERE prior_year_revenue IS NOT NULL
        ORDER BY Region, Corp, Year
        """,
    )


def main() -> None:
    """Build the database and print verification results."""
    args = parse_args()
    database_path = validate_local_database_path(args.database)

    connection = duckdb.connect(str(database_path))
    try:
        rebuild_schema(connection, args.seed)
        verify_and_report(connection)
    finally:
        connection.close()

    print(f"\nDatabase created successfully: {database_path}")
    print("Financial values are synthetic and expressed in whole USD.")


if __name__ == "__main__":
    main()
