# 중국 게임사 재무·출시작 패널

중국 게임사 8곳의 2022~2025년 재무지표 수집 경로와 회사·연도별 대표
출시작 주석을 연결하는 1차 데이터 파이프라인입니다.

## 대상 회사

- A주/BaoStock 자동 수집: 세기화통, 완미세계, 카이잉네트워크, 지비트,
  37인터랙티브엔터테인먼트
- 공식 공시 경로 등록: 텐센트, 넷이즈, 심동회사(XD Inc.)

회사 설정은 `config/china_game_companies.json`, 출시작과 근거 URL은
`config/china_game_releases.json`에 있습니다. 출시작은 완전한 카탈로그가
아니며, 재무 변동을 설명하기 좋은 신규 출시·공개 베타·지역/플랫폼 출시를
회사별 매년 최소 1건 선정했습니다.

## 실행

```bash
python3 -m pip install -r requirements.txt
python3 collect_china_game_financials.py
```

네트워크 호출 없이 설정과 출시작 패널만 다시 만들 수도 있습니다.

```bash
python3 collect_china_game_financials.py --skip-financial
```

## 산출물

- `dataset/companies.csv`: 8개사 식별자, 수집 방식, 공식 공시 경로
- `dataset/a_share_annual_financial_metrics.csv`: A주 5개사 BaoStock 연간지표
- `dataset/representative_releases.csv`: 회사·연도별 대표 출시작과 출처
- `dataset/annual_panel_with_release_notes.csv`: 재무지표와 출시작 주석 결합 패널
- `dataset/provenance.json`: 실행 시각, 건수, 제약사항, 원본 스냅샷 위치

BaoStock 원응답은 추적 가능하도록
`china_game_lake/raw/baostock/run_id=.../responses.jsonl`에 보존되며 Git에는
포함하지 않습니다.

## 해석 주의

BaoStock는 무료이고 키가 필요 없지만 감사 재무제표 전체 계정을 제공하는
Open DART 대체재는 아닙니다. 현재 산출물의 금액은 CNY, 비율은 소수 단위이며
예를 들어 `0.15`는 15%입니다. `profit_MBRevenue`는 BaoStock의 주영업수익
(`MBRevenue`), `profit_netProfit`은 순이익(`netProfit`) 원필드이며 지배주주
귀속 순이익과 반드시 같지는 않습니다. 또한 세기화통·텐센트·넷이즈·심동회사는
게임 외 사업을 포함하므로 `scope_note_ko`를 함께 확인해야 합니다. 최종 투자
분석 전에는 각 행의 `official_filing_url`에서 원 공시와 대조해야 합니다.
