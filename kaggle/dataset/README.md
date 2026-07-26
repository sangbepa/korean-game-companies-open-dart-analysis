# Korean Game Companies Financials 2025–2026

This dataset compares eight Korean listed game publishers using public
consolidated financial statements from the Financial Supervisory Service's
Open DART API.

## Coverage

- Companies: NC, Netmarble, KRAFTON, Kakao Games, Com2uS, Pearl Abyss,
  WEMADE, and Devsisters
- FY2025 annual reports (`reprt_code=11011`)
- Q1 2026 reports with Q1 2025 comparatives (`reprt_code=11013`)
- 2026 disclosure catalog through 2026-07-26
- Currency: KRW
- Statement scope: consolidated (`CFS`)

## Files

- `companies.csv`: company, DART, stock, and market identifiers
- `financial_summary.csv`: one analysis-ready row per company
- `financial_highlights_long.csv`: Open DART standardized major accounts
- `financial_accounts.csv`: full financial-statement account rows
- `disclosures_2026.csv`: filing catalog and public viewer URLs
- `data_dictionary.csv`: definitions, units, and derivations
- `provenance.json`: collection timestamp, endpoints, periods, and row counts
- `key_findings.json`: reproducible headline results

## Methodology

All comparisons use consolidated statements. Annual absolute figures are
compared only with annual figures; Q1 results are compared with the disclosed
prior-year Q1 comparatives. Revenue growth is
`(Q1 2026 - Q1 2025) / Q1 2025`. Operating margin is operating profit divided
by revenue. Debt-to-equity is liabilities divided by equity. Cash-to-assets
uses cash and cash equivalents only and can understate liquidity when a company
holds large short-term financial assets.

Operating-profit percentage changes are not used as the main signal when a
company crosses zero. `q1_profit_direction` identifies turnarounds and turns to
loss explicitly. Extreme growth rates can also reflect a low comparison base,
acquisitions, disposals, or major product launches; this dataset does not assign
causality.

## Source and rights

Source: [Open DART](https://opendart.fss.or.kr/), Financial Supervisory Service,
Republic of Korea. The receipt number and viewer URL are retained for source
verification.

This package is a transformed analytical extract of public disclosure data.
The Kaggle metadata license is set to `Other`. No rights in the underlying
filings are granted by this package. Users must follow the
[Open DART Terms of Use](https://opendart.fss.or.kr/intro/terms.do) and
applicable law. Open DART/FSS and the filing companies do not endorse this
analysis. Figures are presented as filed and may be corrected or restated.

This dataset is for education and research and is not investment advice.
