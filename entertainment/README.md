# Korean Entertainment Companies: Open DART Financial Statements

Open DART에서 한국 주요 엔터테인먼트 상장사 4곳의 연결재무제표를 수집한
데이터셋입니다.

## 범위

- HYBE (KOSPI 352820)
- SM Entertainment (KOSDAQ 041510)
- JYP Entertainment (KOSDAQ 035900)
- YG Entertainment (KOSDAQ 122870)
- 2025년 사업보고서 (`11011`)
- 2026년 1분기보고서 (`11013`)
- 연결재무제표 (`CFS`)

## 파일

- `dataset/companies.csv`: 회사, DART 고유번호, 종목코드, 시장, 표시통화
- `dataset/financial_accounts.csv`: 전체 재무제표 계정
- `dataset/financial_highlights_long.csv`: DART 주요계정
- `dataset/disclosures_2026.csv`: 2026년 정기공시 목록과 원문 링크
- `dataset/provenance.json`: 수집 시각, 범위, 행 수, 통화 메타데이터

모든 금액행의 `currency`는 Open DART 응답값이며, 이번 4개사 데이터는 모두
`KRW`입니다. `reporting_currency`는 회사 설정에 기록한 예상 표시통화로,
API의 `currency`와 대조하는 용도입니다.

Open DART는 통화를 일괄적으로 원화나 달러로 환산하지 않습니다. 외국법인 등
제출인이 USD로 작성한 재무제표는 `currency=USD`로 받을 수 있지만, 이 데이터의
국내 엔터 4사 재무제표는 원화입니다. 달러 비교값이 필요하면 환율 출처와
환산 기준일을 별도로 정해 파생 열로 추가해야 합니다.

## 재수집

```bash
export OPENDART_API_KEY="발급받은_40자리_인증키"

python3 collect_dart.py \
  --companies config/entertainment_companies.json \
  --output dart_data/entertainment \
  --start-date 20260101 \
  --end-date 20260727 \
  --disclosure-type A

python3 collect_financial_data.py \
  --companies config/entertainment_companies.json \
  --raw-root dart_data/entertainment/financial_api \
  --dataset-dir entertainment/dataset \
  --disclosure-catalog dart_data/entertainment/disclosures.jsonl \
  --refresh
```

API 키와 원본 응답은 저장소에 커밋하지 않으며, `dart_data/` 아래에만 둡니다.

## 출처

- [Open DART 단일회사 전체 재무제표 API](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019020)
- [Open DART 재무제표 원본파일(XBRL) API](https://opendart.fss.or.kr/guide/detail.do?apiGrpCd=DS003&apiId=2019019)
