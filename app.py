# === app.py ===
import streamlit as st
import pandas as pd
import snowflake.connector
import re

st.set_page_config(page_title="CRM 매장 SMS 모수 추출", layout="wide")
st.title("📊 CRM 매장 SMS 모수 추출 대시보드")

# ▼▼ 회사 스키마에 맞게 필요시 수정: ACCOUNT 테이블의 CID 컬럼명 ▼▼
CID_COLUMN = "cid__c"  # 예: 'cid__c' (빈값/NULL 제외하여 CSV로만 추출)

# -----------------------------
# Session state 초기화 (선택 누적용)
# -----------------------------
if "results" not in st.session_state:
    st.session_state.results = pd.DataFrame()
if "selected_df" not in st.session_state:
    st.session_state.selected_df = pd.DataFrame(columns=["store_code", "shop_name", "member_cnt"])

# -----------------------------
# Snowflake 연결 (매 쿼리마다 새 연결/종료)
# -----------------------------
def get_connection():
    cfg = st.secrets["snowflake"]
    return snowflake.connector.connect(
        user=cfg["user"],
        password=cfg["password"],
        account=cfg["account"],      # 예: cixxjbf-wp67697
        warehouse=cfg["warehouse"],  # 예: DEV_WH
        database=cfg["database"],    # 예: FNF
        schema=cfg["schema"],        # 예: CRM_MEMBER
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
# (옵션) 연결 점검
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
# 검색 UI
# -----------------------------
brand = st.radio("브랜드 선택", ["X", "M", "I"], index=0, horizontal=True)
kw = st.text_input("매장 검색 키워드 (매장명/번호 일부, 공백·쉼표 복수 입력: 예) 대구, 강남, 501)").strip()
mode = st.radio("검색 토큰 결합 방식", ["하나라도 포함(OR)", "모두 포함(AND)"], index=0, horizontal=True)
do_search = st.button("검색", type="primary")

# -----------------------------
# 검색 로직: 매장명/번호 부분일치 + AND/OR 선택
# (회원 1명 이상 조건은 ACCOUNT 집계로 자연 충족)
# -----------------------------
if do_search:
    if not kw:
        st.warning("키워드를 입력해 주세요.")
        st.session_state.results = pd.DataFrame()
    else:
        try:
            tokens = [t.strip() for t in re.split(r"[,\s]+", kw) if t.strip()]
            params = [brand]
            conds = []
            for t in tokens:
                conds.append("(S.SHOP_NM_SHORT ILIKE %s OR TO_VARCHAR(S.SHOP_ID) ILIKE %s)")
                like = f"%{t}%"
                params.extend([like, like])

            joiner = " OR " if mode.startswith("하나라도") else " AND "
            where_extra = (f" AND ({joiner.join(conds)})") if conds else ""

            sql = (
                "SELECT "
                "  S.SHOP_ID AS STORE_CODE, "
                "  COALESCE(S.SHOP_NM_SHORT, '매장미매핑') AS SHOP_NAME, "
                "  COUNT(*) AS MEMBER_CNT "
                "FROM FNF.CRM_SALESFORCEPROD.ACCOUNT A "
                "LEFT JOIN FNF.PRCS.DB_SHOP S "
                "  ON A.joinstore__c = S.SHOP_ID "
                " AND A.joinbrand__c = S.BRD_CD "
                "WHERE A.joinbrand__c = %s "
                "  AND A.sleep_yn__c = 'N' "
                "  AND A.recv_sms__c = 'Y' "
                f"{where_extra} "
                "GROUP BY 1, 2 "
                "ORDER BY MEMBER_CNT DESC"
            )

            df = run_query(sql, tuple(params))
            df.columns = [c.lower() for c in df.columns]
            st.session_state.results = df
        except Exception as e:
            st.exception(e)

# -----------------------------
# 결과 표시 및 "선택 누적" 컨트롤
# -----------------------------
results = st.session_state.results
if not results.empty:
    st.subheader("검색 결과 (스토어코드 / 매장명 / 모수)")
    st.dataframe(results, use_container_width=True)

    options = [f"{r.store_code} | {r.shop_name} ({int(r.member_cnt):,})" for r in results.itertuples(index=False)]
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
        st.session_state.selected_df = pd.DataFrame(columns=["store_code", "shop_name", "member_cnt"])

    if remove_some and picked_codes:
        keep_mask = ~st.session_state.selected_df["store_code"].astype(str).isin([str(c) for c in picked_codes])
        st.session_state.selected_df = st.session_state.selected_df[keep_mask]

# -----------------------------
# 누적 선택 목록 & 합계 / CSV & CID 추출 (CID는 화면에 미노출)
# -----------------------------
sel_df = st.session_state.selected_df
if not sel_df.empty:
    st.subheader("누적 선택 매장")
    st.dataframe(sel_df, use_container_width=True)

    total_cnt = int(sel_df["member_cnt"].sum())
    st.success(f"✅ 총 모수: {total_cnt:,} 명")  # 화면에는 합계만 표시 (CID 미노출)

    # 선택 매장 요약 CSV (코드/이름/모수)
    csv = sel_df.to_csv(index=False).encode("utf-8-sig")
    st.download_button("선택 매장 리스트 CSV", data=csv, file_name="sms_target_counts_selected.csv", mime="text/csv")

    st.divider()
    st.subheader("📤 선택 매장 CID 추출 (CSV 다운로드 전용)")
    if st.button("선택 매장 CID 추출"):
        try:
            codes = [str(c) for c in sel_df["store_code"].astype(str).tolist()]
            if len(codes) == 0:
                st.info("선택된 매장이 없습니다.")
            else:
                placeholders = ",".join(["%s"] * len(codes))
                # ACCOUNT에서 바로 추출 + 빈값/NULL 제거 (삼중따옴표 없이 안전한 조립)
                sql_cid = (
                    "SELECT DISTINCT A.{cid} AS CID "
                    "FROM FNF.CRM_SALESFORCEPROD.ACCOUNT A "
                    "WHERE A.joinbrand__c = %s "
                    "AND A.sleep_yn__c = 'N' "
                    "AND A.recv_sms__c = 'Y' "
                    "AND A.{cid} IS NOT NULL "
                    "AND LENGTH(TRIM(A.{cid})) > 0 "
                    f"AND A.joinstore__c IN ({placeholders})"
                ).format(cid=CID_COLUMN)

                params = tuple([brand] + codes)
                cid_df = run_query(sql_cid, params)

                if cid_df.empty:
                    st.info("선택 매장에서 조건을 만족하는 CID가 없습니다.")
                else:
                    st.write(f"CID 개수: **{len(cid_df):,}**")
                    cid_csv = cid_df.to_csv(index=False).encode("utf-8-sig")  # 한 컬럼: CID
                    st.download_button("CID CSV 다운로드", data=cid_csv, file_name="cid_list.csv", mime="text/csv")
        except Exception as e:
            st.exception(e)

st.caption("※ 화면엔 합계만 표시 · CID는 CSV로만 제공 / 조건: 수신동의(Y) & 휴면(N) / 선택 누적 & AND·OR 검색 지원")
