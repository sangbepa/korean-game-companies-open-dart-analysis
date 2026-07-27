# Korean Food Companies: Open DART Financial Analysis

Open DART에서 국내 주요 식품 상장사 4곳의 연결재무제표를 수집하고 기본
재무비율을 비교한 데이터셋입니다.

## 범위

- Samyang Foods (KOSPI 003230)
- Nongshim (KOSPI 004370)
- Orion (KOSPI 271560)
- Ottogi (KOSPI 007310)
- 2025년 사업보고서 (`11011`)
- 2026년 1분기보고서 (`11013`)
- 연결재무제표 (`CFS`), 원화(`KRW`)

## 파일

- `dataset/companies.csv`: 회사 및 DART 식별자
- `dataset/financial_accounts.csv`: 전체 재무제표 계정
- `dataset/financial_highlights_long.csv`: DART 주요계정
- `dataset/disclosures_2026.csv`: 수집한 정기공시 목록
- `dataset/provenance.json`: 수집 범위와 행 수
- `analysis/basic_financial_summary.csv`: 기본 재무분석 결과

## 계산 정의

- 매출 성장률: 전년 동기 대비 매출 증감률
- 매출총이익률: 매출총이익 / 매출
- 영업이익률: 영업이익 / 매출
- 부채비율: 부채총계 / 자본총계
- 유동비율: 유동자산 / 유동부채
- ROE: 당기순이익 / 평균 자본(연간 데이터만 계산)
- 재고일수: 평균 재고자산 / 매출원가 × 365(연간 데이터만 계산)
- 간이 FCF: 영업활동현금흐름 - 유형자산 취득액

간이 FCF에는 무형자산 취득, 유형자산 처분대금 및 사업결합 현금흐름이
포함되지 않으므로 정식 잉여현금흐름과 다를 수 있습니다.

## 핵심 관찰

- Samyang Foods: 가장 빠른 성장과 가장 높은 영업이익률. 2025년 대규모
  유형자산 투자로 간이 FCF는 음수였으나 2026년 1분기에는 크게 회복했습니다.
- Orion: 높은 수익성과 가장 낮은 부채비율, 가장 큰 연간 간이 FCF를 함께
  보여 재무구조가 가장 안정적입니다.
- Nongshim: 성장률은 낮지만 2026년 1분기 수익성과 현금흐름이 개선됐습니다.
- Ottogi: 매출 규모는 가장 크지만 수익성과 자본효율이 가장 낮고, 설비투자 후
  남는 현금이 적습니다.

## 재수집

```bash
export OPENDART_API_KEY="발급받은_40자리_인증키"

python3 collect_dart.py \
  --companies config/food_companies.json \
  --output dart_data/food \
  --start-date 20260101 \
  --end-date 20260727 \
  --disclosure-type A

python3 collect_financial_data.py \
  --companies config/food_companies.json \
  --raw-root dart_data/food/financial_api \
  --dataset-dir food/dataset \
  --disclosure-catalog dart_data/food/disclosures.jsonl \
  --refresh
```

## 출처

- [Open DART 단일회사 전체 재무제표 API](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020)
