# === app.py ===
import streamlit as st
import pandas as pd
import snowflake.connector
import re
from datetime import date, timedelta

st.set_page_config(page_title="CRM 매장 SMS 모수 추출", layout="wide")

# --- 비밀번호 게이트 (Secrets의 [app].password 사용) ---
def require_password():
    if "app" not in st.secrets or not st.secrets["app"].get("password"):
        return
    if st.session_state.get("pw_ok"):
        return
    with st.form("pw_form", clear_on_submit=False):
        pwd = st.text_input("비밀번호를 입력하세요", type="password")
        submitted = st.form_submit_button("입장")
    if submitted:
        if pwd == st.secrets["app"]["password"]:
            st.session_state["pw_ok"] = True
            if hasattr(st, "rerun"):
                st.rerun()
            else:
                st.experimental_rerun()
        else:
            st.error("비밀번호가 틀렸습니다.")
            st.stop()
    else:
        st.stop()

require_password()

st.title("📊 CRM 매장 SMS 모수 추출 대시보드")

# ▼ ACCOUNT 테이블의 CID 컬럼명
CID_COLUMN = "cid__c"

# -----------------------------
# Session state 초기화
# -----------------------------
if "results" not in st.session_state:
    st.session_state.results = pd.DataFrame()
if "selected_df" not in st.session_state:
    st.session_state.selected_df = pd.DataFrame(
        columns=["store_code", "shop_name", "member_cnt", "purchaser_cnt", "total_cnt"]
    )

# -----------------------------
# Snowflake 연결/쿼리
# -----------------------------
def get_connection():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],
        warehouse=cfg["warehouse"],
        database=cfg["database"],
        schema=cfg["schema"],
        role=cfg.get("role"),
    )

@st.cache_data(show_spinner=True, ttl=300)
def run_query(sql: str, params: tuple | None = None) -> pd.DataFrame:
    conn = get_connection()
    try:
        cur = conn.cursor()
        try:
            cur.execute(sql, params)
            df = cur.fetch_pandas_all()
        finally:
            cur.close()
    finally:
        conn.close()
    return df

# -----------------------------
# 연결 점검
# -----------------------------
with st.expander("🔌 연결 테스트"):
    if st.button("Snowflake 연결 확인"):
        try:
            df_ctx = run_query(
                "SELECT CURRENT_ROLE() AS ROLE, CURRENT_WAREHOUSE() AS WH, "
                "CURRENT_DATABASE() AS DB, CURRENT_SCHEMA() AS SCH;"
            )
            st.dataframe(df_ctx, use_container_width=True)
            st.success("연결 정상 ✅")
        except Exception as e:
            st.exception(e)

# -----------------------------
# 검색 UI (브랜드 다중선택 → 기간 → 키워드 → 검색)
# -----------------------------
brands = st.multiselect("브랜드 선택(복수 선택 가능)", ["X", "M", "I"], default=["X"])
# 기간 먼저
default_start = date.today() - timedelta(days=30)
default_end = date.today()
col1, col2 = st.columns([1, 2])
with col1:
    all_time = st.checkbox("전체기간(제한 없음)", value=False)
with col2:
    buy_start, buy_end = st.date_input(
        "구매 기간(구매 인원 집계에 적용)",
        (default_start, default_end),
        disabled=all_time
    )
# 키워드
kw = st.text_input("매장 관련 정보를 입력하세요! 지역, 매장명, 매장코드 등").strip()

# 검색 버튼
do_search = st.button("검색", type="primary")

# -----------------------------
# 검색 로직
# -----------------------------
if do_search:
    if not brands:
        st.warning("브랜드를 1개 이상 선택해 주세요.")
        st.session_state.results = pd.DataFrame()
    elif not kw:
        st.warning("키워드를 입력해 주세요.")
        st.session_state.results = pd.DataFrame()
    else:
        try:
            # 키워드 토큰 (항상 OR)
            tokens = [t.strip() for t in re.split(r"[,\s]+", kw) if t.strip()]
            joiner = " OR "
            conds, token_params = [], []
            for t in tokens:
                conds.append("(S.SHOP_NM_SHORT ILIKE %s OR TO_VARCHAR(S.SHOP_ID) ILIKE %s)")
                like = f"%{t}%"
                token_params.extend([like, like])
            token_filter_sql = (f" AND ({joiner.join(conds)})") if conds else ""

            # 브랜드 IN 절
            brand_ph = ",".join(["%s"] * len(brands))

            # 기간 필터
            sale_dt_filter_sql = "" if all_time else "AND SL.SALE_DT BETWEEN %s AND %s"
            date_params = [] if all_time else [str(buy_start), str(buy_end)]

            # 파라미터: M용 [brands] + token(M) + P용 [brands] + date + token(P)
            params = []
            params.extend(brands)               # M
            params.extend(token_params)         # token(M)
            params.extend(brands)               # P
            params.extend(date_params)          # dates
            params.extend(token_params)         # token(P)

            sql = f"""
WITH M AS (
  SELECT 
      S.SHOP_ID AS SHOP_ID,
      COALESCE(S.SHOP_NM_SHORT, '매장미매핑') AS SHOP_NAME,
      A.{CID_COLUMN} AS CID
  FROM FNF.CRM_SALESFORCEPROD.ACCOUNT A
  LEFT JOIN FNF.PRCS.DB_SHOP S
    ON A.joinstore__c = S.SHOP_ID
   AND A.joinbrand__c = S.BRD_CD
  WHERE A.joinbrand__c IN ({brand_ph})
    AND A.sleep_yn__c = 'N'
    AND A.recv_sms__c = 'Y'
    AND A.status_cd__c = 'R'               -- ✅ R만 포함
    AND A.{CID_COLUMN} IS NOT NULL
    AND LENGTH(TRIM(A.{CID_COLUMN})) > 0
    {token_filter_sql}
),
P AS (
  SELECT DISTINCT
      S.SHOP_ID AS SHOP_ID,
      COALESCE(S.SHOP_NM_SHORT, '매장미매핑') AS SHOP_NAME,
      A.{CID_COLUMN} AS CID
  FROM FNF.PRCS.DW_SALE SL
  JOIN FNF.PRCS.DB_SHOP S
    ON SL.BRD_CD = S.BRD_CD
   AND SL.SHOP_ID = S.SHOP_ID
  JOIN FNF.CRM_SALESFORCEPROD.ACCOUNT A
    ON A.{CID_COLUMN} = SL.CUST_ID
   AND A.joinbrand__c = SL.BRD_CD
  WHERE SL.BRD_CD IN ({brand_ph})
    {sale_dt_filter_sql}
    AND A.sleep_yn__c = 'N'
    AND A.recv_sms__c = 'Y'
    AND A.status_cd__c = 'R'               -- ✅ R만 포함
    AND A.{CID_COLUMN} IS NOT NULL
    AND LENGTH(TRIM(A.{CID_COLUMN})) > 0
    {token_filter_sql}
),
PO AS ( -- Purchasers Only: 가입자(M)와 중복되지 않는 구매자
  SELECT P.SHOP_ID, P.SHOP_NAME, P.CID
  FROM P
  LEFT JOIN M
    ON M.SHOP_ID = P.SHOP_ID
   AND M.CID = P.CID
  WHERE M.CID IS NULL
)
SELECT
  X.SHOP_ID     AS STORE_CODE,
  X.SHOP_NAME   AS SHOP_NAME,
  COUNT(DISTINCT CASE WHEN X.SRC = 'M'  THEN X.CID END) AS MEMBER_CNT,
  COUNT(DISTINCT CASE WHEN X.SRC = 'PO' THEN X.CID END) AS PURCHASER_CNT,
  COUNT(DISTINCT X.CID) AS TOTAL_CNT
FROM (
  SELECT 'M'  AS SRC, SHOP_ID, SHOP_NAME, CID FROM M
  UNION ALL
  SELECT 'PO' AS SRC, SHOP_ID, SHOP_NAME, CID FROM PO
) X
GROUP BY 1,2
ORDER BY TOTAL_CNT DESC, MEMBER_CNT DESC, PURCHASER_CNT DESC
            """
            df = run_query(sql, tuple(params))
            df.columns = [c.lower() for c in df.columns]
            st.session_state.results = df
        except Exception as e:
            st.exception(e)

# -----------------------------
# 결과 표시 & 선택 누적
# -----------------------------
results = st.session_state.results
if not results.empty:
    st.subheader("검색 결과 (스토어코드 / 매장명 / 가입 / 구매(가입제외) / 합계)")

    results_display = results.rename(
        columns={"member_cnt": "가입", "purchaser_cnt": "구매(가입제외)", "total_cnt": "합계"}
    )
    st.dataframe(results_display, use_container_width=True)

    options = [
        f"{r.store_code} | {r.shop_name} (가입 {int(r.member_cnt):,} / 구매 {int(r.purchaser_cnt):,} / 합계 {int(r.total_cnt):,})"
        for r in results.itertuples(index=False)
    ]
    pick = st.multiselect("발송 대상 매장 선택 (현재 검색결과에서 추가)", options)

    col_a, col_b, col_c = st.columns([1, 1, 1])
    with col_a:
        add_now = st.button("선택 추가 ➕", use_container_width=True)
    with col_b:
        clear_sel = st.button("선택 초기화 ♻️", use_container_width=True)
    with col_c:
        remove_some = st.button("체크한 항목 제거 ➖", use_container_width=True)

    picked_codes = [p.split(" | ")[0] for p in pick]

    if add_now and picked_codes:
        add_df = results[results["store_code"].isin(picked_codes)]
        st.session_state.selected_df = (
            pd.concat([st.session_state.selected_df, add_df], ignore_index=True)
            .drop_duplicates(subset=["store_code"], keep="last")
        )

    if clear_sel:
        st.session_state.selected_df = pd.DataFrame(
            columns=["store_code", "shop_name", "member_cnt", "purchaser_cnt", "total_cnt"]
        )

    if remove_some and picked_codes:
        keep_mask = ~st.session_state.selected_df["store_code"].astype(str).isin([str(c) for c in picked_codes])
        st.session_state.selected_df = st.session_state.selected_df[keep_mask]

# -----------------------------
# 누적 선택 & 합계 / CSV & user_id 추출(세트 선택)
# -----------------------------
sel_df = st.session_state.selected_df
if not sel_df.empty:
    st.subheader("누적 선택 매장")

    # 표는 한글 라벨로 노출
    display_df = sel_df.copy()
    display_df["가입"] = display_df["member_cnt"].astype(int)
    display_df["구매(가입제외)"] = display_df["purchaser_cnt"].astype(int)
    display_df["합계"] = display_df["total_cnt"].astype(int)

    # 합계(숫자)
    total_member = int(sel_df["member_cnt"].sum())
    total_buyer_only = int(sel_df["purchaser_cnt"].sum())
    total_sum = int(sel_df["total_cnt"].sum())

    # 합계 행
    sum_row = pd.DataFrame(
        {"store_code": ["합계"], "shop_name": ["-"], "가입": [total_member],
         "구매(가입제외)": [total_buyer_only], "합계": [total_sum]}
    )

    # 문자 발송비용 행 (합계 × 23.5원)
    LMS_UNIT = 23.5
    cost_row = pd.DataFrame(
        {
            "store_code": ["문자 발송비용(원)"],
            "shop_name": ["-"],
            "가입": [f"{total_member * LMS_UNIT:,.1f}"],
            "구매(가입제외)": [f"{total_buyer_only * LMS_UNIT:,.1f}"],
            "합계": [f"{total_sum * LMS_UNIT:,.1f}"],
        }
    )

    render_cols = ["store_code", "shop_name", "가입", "구매(가입제외)", "합계"]
    sel_show = pd.concat([display_df[render_cols], sum_row, cost_row], ignore_index=True)
    st.dataframe(sel_show, use_container_width=True)

    # 상단 요약 및 비용 총액 안내
    st.success(f"✅ 총(가입): {total_member:,} | 🛒 총(구매, 가입중복제외): {total_buyer_only:,} | Σ 합계: {total_sum:,}")
    st.info(f"💬 LMS 발송 비용(예상): 합계 {total_sum:,} × 23.5원 = **{total_sum * LMS_UNIT:,.1f}원**")

    # 선택 매장 요약 CSV (원본 컬럼 유지)
    csv = sel_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("선택 매장 리스트 CSV", data=csv, file_name="sms_target_counts_selected.csv", mime="text/csv")

    st.divider()
    st.subheader("📤 선택 매장 user_id(CID) 추출")
    cohort = st.radio("어떤 세트를 추출할까요?", ["가입자", "구매자(가입중복제외)", "합계"], index=2, horizontal=True)

    if st.button("user_id 추출(CSV)"):
        try:
            codes = [str(c) for c in sel_df["store_code"].astype(str).tolist()]
            if len(codes) == 0:
                st.info("선택된 매장이 없습니다.")
            else:
                placeholders_codes = ",".join(["%s"] * len(codes))
                placeholders_brands = ",".join(["%s"] * len(brands))
                sale_dt_filter_uid = "" if all_time else "AND SL.SALE_DT BETWEEN %s AND %s"
                date_params_uid = [] if all_time else [str(buy_start), str(buy_end)]

                sql_uid = (
                    f"""
WITH M AS (
  SELECT 
      S.SHOP_ID AS SHOP_ID,
      A.{CID_COLUMN} AS CID
  FROM FNF.CRM_SALESFORCEPROD.ACCOUNT A
  JOIN FNF.PRCS.DB_SHOP S
    ON A.joinstore__c = S.SHOP_ID
   AND A.joinbrand__c = S.BRD_CD
  WHERE A.joinbrand__c IN ({placeholders_brands})
    AND S.SHOP_ID IN ({placeholders_codes})
    AND A.sleep_yn__c = 'N'
    AND A.recv_sms__c = 'Y'
    AND A.status_cd__c = 'R'
    AND A.{CID_COLUMN} IS NOT NULL
    AND LENGTH(TRIM(A.{CID_COLUMN})) > 0
),
P AS (
  SELECT DISTINCT
      S.SHOP_ID AS SHOP_ID,
      A.{CID_COLUMN} AS CID
  FROM FNF.PRCS.DW_SALE SL
  JOIN FNF.PRCS.DB_SHOP S
    ON SL.BRD_CD = S.BRD_CD
   AND SL.SHOP_ID = S.SHOP_ID
  JOIN FNF.CRM_SALESFORCEPROD.ACCOUNT A
    ON A.{CID_COLUMN} = SL.CUST_ID
   AND A.joinbrand__c = SL.BRD_CD
  WHERE SL.BRD_CD IN ({placeholders_brands})
    AND S.SHOP_ID IN ({placeholders_codes})
    {sale_dt_filter_uid}
    AND A.sleep_yn__c = 'N'
    AND A.recv_sms__c = 'Y'
    AND A.status_cd__c = 'R'
    AND A.{CID_COLUMN} IS NOT NULL
    AND LENGTH(TRIM(A.{CID_COLUMN})) > 0
),
PO AS (
  SELECT P.SHOP_ID, P.CID
  FROM P
  LEFT JOIN M
    ON M.SHOP_ID = P.SHOP_ID
   AND M.CID = P.CID
  WHERE M.CID IS NULL
)
"""
                    +
                    (
                        "SELECT DISTINCT CID AS user_id FROM M"
                        if cohort.startswith("가입자")
                        else "SELECT DISTINCT CID AS user_id FROM PO"
                        if cohort.startswith("구매자")
                        else "SELECT DISTINCT CID AS user_id FROM (SELECT CID FROM M UNION ALL SELECT CID FROM PO) U"
                    )
                )

                # params: brands + codes + brands + codes + dates
                params_uid = brands + codes + brands + codes + date_params_uid
                uid_df = run_query(sql_uid, tuple(params_uid))

                if uid_df.empty:
                    st.info("선택 조건에 해당하는 user_id가 없습니다.")
                else:
                    st.write(f"user_id 개수: **{len(uid_df):,}**")
                    uid_csv = uid_df.to_csv(index=False).encode("utf-8-sig")
                    st.download_button(
                        "user_id CSV 다운로드",
                        data=uid_csv,
                        file_name=(
                            "user_id_members.csv" if cohort.startswith("가입자")
                            else "user_id_purchasers_only.csv" if cohort.startswith("구매자")
                            else "user_id_total.csv"
                        ),
                        mime="text/csv"
                    )
        except Exception as e:
            st.exception(e)

st.caption(
    "※ 화면엔 합계만 표시 · user_id(CID)는 CSV로만 제공 / 조건: 수신동의(Y) & 휴면(N) & 상태코드 R만 포함 / "
    "구매 인원은 설정 기간 내 구매 기준이며 가입자와 중복 제외 / 합계=가입 ∪ 구매(가입중복제외) / "
    "LMS 비용은 1건당 23.5원 기준 예상치"
)
