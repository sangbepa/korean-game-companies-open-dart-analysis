# 일본 게임사 재무·출시작 패널

캡콤을 첫 대상 회사로 삼은 일본 게임사 수집 파이프라인입니다. 회사 공식
IR의 구조화된 재무표를 정규화하고, 회사 회계연도별 대표 출시작 주석과
유가증권보고서 원본을 연결합니다.

## 회계연도 기준

캡콤의 `FY2022`는 2022년 4월 1일부터 2023년 3월 31일까지입니다. 따라서
2025년 2월 28일 출시된 《Monster Hunter Wilds》는 `FY2024`에 들어갑니다.
`fiscal_year_end`와 `release_date`를 함께 보면 캘린더 연도 혼동을 피할 수
있습니다.

## 실행

```bash
python3 collect_japan_game_financials.py
```

기본 실행은 API 키 없이 다음 자료를 수집합니다.

- 캡콤 공식 IR의 Euroland 구조화 재무표
- 캡콤 IR에 게시된 FY2022~FY2025 유가증권보고서 PDF
- 회사·회계연도별 대표 출시작과 공식 발표 URL

네트워크 호출 없이 설정과 대표작 패널만 검증하려면 다음과 같이 실행합니다.

```bash
python3 collect_japan_game_financials.py --skip-financial
```

EDINET API 메타데이터를 추가하려면 발급받은 키를 로컬 키 파일에 한 번만
저장합니다. `.secrets/` 전체는 Git에서 제외됩니다.

```bash
mkdir -p .secrets
chmod 700 .secrets
read -s "EDINET_API_KEY?EDINET API key: "
print -rn -- "$EDINET_API_KEY" > .secrets/edinet_api_key
chmod 600 .secrets/edinet_api_key
unset EDINET_API_KEY
```

이후에는 키를 다시 입력하지 않고 실행할 수 있습니다.

```bash
python3 collect_japan_game_financials.py --edinet-enrich
```

환경변수 `EDINET_API_KEY`가 설정돼 있으면 로컬 파일보다 우선합니다. 키 값은
산출물, 원본 매니페스트 또는 로그에 저장하지 않습니다.

## 산출물

- `dataset/companies.csv`: 회사 식별자와 공시 경로
- `dataset/annual_financial_metrics.csv`: 연결 재무·현금흐름·디지털 콘텐츠 세그먼트 지표
- `dataset/representative_releases.csv`: 회계연도별 대표작과 공식 근거 URL
- `dataset/annual_panel_with_release_notes.csv`: 재무지표와 대표작을 결합한 패널
- `dataset/edinet_documents.csv`: 선택 실행 시 수집한 EDINET 연차보고서 메타데이터
- `dataset/provenance.json`: 실행 건수, 원본 위치, 해석상 제약

원본 HTML과 유가증권보고서 PDF는
`japan_game_lake/raw/.../run_id=.../` 아래에 보존하며 Git에는 포함하지 않습니다.
`--edinet-enrich` 실행 시 EDINET이 제공하는 문서별 XBRL ZIP도 같은 실행 폴더의
`edinet/` 아래에 보존합니다.

## 지표 단위와 주의점

금액 열의 접미사 `_m_jpy`는 백만 엔입니다. `operating_margin`,
`net_margin`, `digital_contents_operating_margin` 등의 비율은 소수 단위입니다.
예를 들어 `0.4`는 40%입니다. 캡콤 연결 실적에는 아케이드와 오락기기 사업도
포함되므로 콘솔 게임 분석에는 디지털 콘텐츠 세그먼트 열을 함께 사용해야 합니다.
