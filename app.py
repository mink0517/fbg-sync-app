import io
import re
from dateutil import parser as dtparser


import numpy as np
import pandas as pd
import streamlit as st
BAT_CACHE_VER = 2
FBG_CACHE_VER = 2


# =========================================================
# 캐시
# =========================================================
@st.cache_data(show_spinner=False)
def parse_bat_cached(file_bytes: bytes, cache_ver: int) -> pd.DataFrame:
    return parse_battery_excel(file_bytes)


@st.cache_data(show_spinner=False)
def parse_fbg_cached(file_bytes: bytes, cache_ver: int) -> pd.DataFrame:
    return parse_fbg_txt_fixed4(file_bytes)


with st.sidebar:
    if st.button("캐시 초기화"):
        st.cache_data.clear()
        st.rerun()




# =========================================================
# 1) 배터리 엑셀 파싱
# - "Absolute Time" 원본(ms 포함)을 유지: battery_time_raw
# - UI도 이 값을 그대로 사용(18:04:32.305 같은 값이 그대로 보이게)
# =========================================================
def parse_battery_excel(xlsx_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(xlsx_bytes), sheet_name="Detail_Layer_Data", engine="openpyxl")


    if "Absolute Time" not in df.columns:
        raise ValueError("Detail_Layer_Data 시트에 'Absolute Time' 컬럼이 없습니다.")


    for c in ["Current(mA)", "Voltage(V)"]:
        if c not in df.columns:
            raise ValueError(f"배터리 데이터에 '{c}' 컬럼이 없습니다.")


    # ✅ 원본 시간(ms 포함) 파싱
    df["battery_time_raw"] = pd.to_datetime(df["Absolute Time"], errors="coerce")
    # 혹시 문자열/특수 포맷 섞인 경우 보강
    mask = df["battery_time_raw"].isna()
    if mask.any():
        df.loc[mask, "battery_time_raw"] = pd.to_datetime(
            df.loc[mask, "Absolute Time"].astype(str).apply(dtparser.parse),
            errors="coerce"
        )


    df = df.dropna(subset=["battery_time_raw"]).copy()


    df["Current_mA"] = pd.to_numeric(df["Current(mA)"], errors="coerce")
    df["Voltage_V"] = pd.to_numeric(df["Voltage(V)"], errors="coerce")
    df = df.dropna(subset=["Current_mA", "Voltage_V"]).copy()


    df = df.sort_values("battery_time_raw").reset_index(drop=True)
    df = df.drop_duplicates("battery_time_raw", keep="first").reset_index(drop=True)


    # ✅ 이 3개 컬럼을 "반드시" 반환
    return df[["battery_time_raw", "Current_mA", "Voltage_V"]]






# =========================================================
# 2) FBG txt 파싱 (채널 4개 고정 + 센서 후보 1~4 고정)
#
# txt 구조:
#   ... header ...
#   Timestamp   # CH 1  # CH 2  # CH 3  # CH 4
#   1/31/2026 23:59:42.93575   2 2 1 3  <signals...>
#
# 요구사항:
# - 각 행마다 c1~c4가 바뀔 수 있음
# - UI는 항상 CH1_1..CH4_4까지 선택 가능하게 하고 싶음
# - 그래서 컬럼은 무조건 4개까지 만들어두고(없으면 NaN),
#   counts가 4 초과면 4까지만 사용(나머지는 버림)
# =========================================================
def parse_fbg_txt_fixed4(txt_bytes: bytes) -> pd.DataFrame:
    text = txt_bytes.decode("utf-8", errors="ignore")
    lines = [ln.rstrip("\n") for ln in text.splitlines() if ln.strip()]


    # 헤더 찾기
    header_idx = None
    for i, ln in enumerate(lines):
        if ln.strip().startswith("Timestamp") and ("CH 1" in ln or "# CH 1" in ln):
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("FBG txt에서 'Timestamp' 테이블 헤더를 찾지 못했습니다.")


    data_lines = lines[header_idx + 1 :]


    date_pat = re.compile(r"^\d{1,2}/\d{1,2}/\d{4}\s+\d{2}:\d{2}:\d{2}\.\d+")
    rows = []


    for ln in data_lines:
        ln = ln.strip()
        if not date_pat.match(ln):
            continue


        parts = re.split(r"\s+", ln)
        if len(parts) < 6:
            continue


        ts_str = parts[0] + " " + parts[1]
        try:
            ts = dtparser.parse(ts_str)
        except Exception:
            continue


        try:
            c1, c2, c3, c4 = map(int, parts[2:6])
        except Exception:
            continue


        # 실제 센서 수(행별) 합
        total = c1 + c2 + c3 + c4
        sig_strs = parts[6:]
        if len(sig_strs) < total:
            continue


        try:
            sig_vals = list(map(float, sig_strs[:total]))
        except Exception:
            continue


        # ✅ 4개까지만 사용
        c1u, c2u, c3u, c4u = min(c1, 4), min(c2, 4), min(c3, 4), min(c4, 4)


        rec = {
            "fbg_time_raw": pd.Timestamp(ts),
            "CH1_count": c1,
            "CH2_count": c2,
            "CH3_count": c3,
            "CH4_count": c4,
        }


        idx = 0
        # CH1
        for i in range(c1):
            if i < 4:
                rec[f"CH1_{i+1}"] = sig_vals[idx]
            idx += 1
        # CH2
        for i in range(c2):
            if i < 4:
                rec[f"CH2_{i+1}"] = sig_vals[idx]
            idx += 1
        # CH3
        for i in range(c3):
            if i < 4:
                rec[f"CH3_{i+1}"] = sig_vals[idx]
            idx += 1
        # CH4
        for i in range(c4):
            if i < 4:
                rec[f"CH4_{i+1}"] = sig_vals[idx]
            idx += 1


        # ✅ 없는 컬럼은 NaN으로 채워서 “항상 1~4”가 존재하게
        for ch in [1, 2, 3, 4]:
            for si in [1, 2, 3, 4]:
                rec.setdefault(f"CH{ch}_{si}", np.nan)


        rows.append(rec)


    if not rows:
        raise ValueError("FBG txt에서 데이터 행 파싱 실패: 형식/구분자 확인 필요")


    df = pd.DataFrame(rows).sort_values("fbg_time_raw").reset_index(drop=True)
    # 중복 timestamp면 첫 행 유지
    df = df.drop_duplicates("fbg_time_raw", keep="first").reset_index(drop=True)
    return df




# =========================================================
# 3) 시작 배터리 시간에 대해 FBG 시작 index를 nearest로 1번만 정함
# - 허용오차 UI 없음 (요구사항 반영)
# =========================================================
def find_nearest_index(time_series: pd.Series, target: pd.Timestamp) -> int:
    # time_series: datetime64
    deltas = (time_series - target).abs()
    return int(deltas.idxmin())




# =========================================================
# 4) T/S 계산 (PPT 식 반영)
# - dt_pm = (temp - temp0) * 1000
# - ds_pm = (vol  - vol0 ) * 1000
# - T = dt_pm / 9.2
# - S = (ds_pm - dt_pm) / 0.83    ✅ 중요(단위 일치)
# =========================================================
def compute_T_S(df: pd.DataFrame, temp_col: str, vol_col: str) -> pd.DataFrame:
    out = df.copy()


    out[temp_col] = pd.to_numeric(out[temp_col], errors="coerce")
    out[vol_col] = pd.to_numeric(out[vol_col], errors="coerce")


    if out[temp_col].isna().all():
        raise ValueError(f"선택한 온도 센서({temp_col})가 구간 내에 유효값이 없습니다.")
    if out[vol_col].isna().all():
        raise ValueError(f"선택한 부피 센서({vol_col})가 구간 내에 유효값이 없습니다.")


    # 시작값(첫 행) 기준
    temp0 = out[temp_col].iloc[0]
    vol0 = out[vol_col].iloc[0]


    dt_pm = (out[temp_col] - temp0) * 1000.0
    ds_pm = (out[vol_col] - vol0) * 1000.0

    T_raw = dt_pm / 9.2
    S_raw = (ds_pm - dt_pm) / 0.83

    # ✅ 중간값도 컬럼으로 남기기
    out["dt_pm"] = dt_pm      # 9.2 적용 전 (온도 파장변화량*1000)
    out["ds_pm"] = ds_pm      # 0.83 적용 전 (부피 파장변화량*1000)
    out["T_raw"] = T_raw      # 9.2 적용 후
    out["S_raw"] = S_raw      # 0.83 적용 후
    return out





# =========================================================
# 5) forward smoothing
# - 시작 행을 따로 받지 않음: "잘라낸 구간의 첫 행이 엑셀 2행" 개념
# - window=39면: 1행 값은 1~39 평균(엑셀로 치면 2~40 평균과 동일한 개념)
# =========================================================
def forward_rolling_mean(series: pd.Series, window_size: int) -> pd.Series:
    arr = pd.to_numeric(series, errors="coerce").to_numpy(dtype=float)
    n = len(arr)
    y = np.full(n, np.nan, dtype=float)


    for i in range(n):
        j = i + window_size
        if j > n:
            break
        y[i] = float(np.nanmean(arr[i:j]))


    return pd.Series(y, index=series.index, dtype=float)




# =========================================================
# 6) 최종 출력
# - 배터리 원본 시간(ms) + FBG 원본 시간(ms) 둘 다 출력
# - Time(h)는 선택 구간 시작점 기준 0부터
# =========================================================
def build_final_output(df: pd.DataFrame) -> pd.DataFrame:
    t0 = df["battery_time_raw"].iloc[0]
    elapsed_s = (df["battery_time_raw"] - t0).dt.total_seconds()


    out = pd.DataFrame({
        "Battery_Timestamp": df["battery_time_raw"],
        "FBG_Timestamp": df["fbg_time_raw"],
        "Time(h)": elapsed_s / 3600.0,
        "Current(mA)": df["Current_mA"],
        "Voltage(V)": df["Voltage_V"],

        
        "dt": df["dt_pm"],   # (temp-temp0)*1000
        "ds": df["ds_pm"],   # (vol-vol0)*1000
        

        # ✅ 9.2 / 0.83 적용 후 값(중간 결과)
        "t_delta": df["T_raw"],   # dt/9.2
        "s_delta": df["S_raw"],   # (ds - t_delta)/0.83  (현재 코드 정의 기준)

        # ✅ 스무딩 최종값
        "T": df["T_smooth"],
        "S": df["S_smooth"],
    })


    return out




# =========================================================
# Streamlit UI (2-step: 선택 → 한번에 실행/다운로드)
# =========================================================
st.set_page_config(page_title="FBG-배터리 싱크 자동화", layout="wide")
st.title("FBG(txt) + 배터리(xlsx) 싱크/가공 → 결과 다운로드")


st.markdown(
"""
### 처리 방식(요구사항 반영)
- **시간 범위 선택은 배터리 원본 시간(ms 포함) 기준**
- FBG는 **시작점만 nearest로 정하고**, 이후는 **행 기준 1:1 매칭**
- 채널은 **CH1~CH4 고정**, 센서 번호는 **1~4 고정 입력**
- 스무딩은 **선택 구간 첫 행부터 forward 평균(window행)**

"""
)


fbg_file = st.file_uploader("1) FBG txt 업로드", type=["txt"])
bat_file = st.file_uploader("2) 배터리 xlsx 업로드", type=["xlsx"])


if not (fbg_file and bat_file):
    st.info("FBG txt와 배터리 xlsx를 둘 다 업로드하면 설정이 열립니다.")
    st.stop()


try:
    bat = parse_bat_cached(bat_file.getvalue(), BAT_CACHE_VER)
    fbg = parse_fbg_cached(fbg_file.getvalue(), FBG_CACHE_VER)


except Exception as e:
    st.error(f"파싱 오류: {e}")
    st.stop()


# 배터리 시간 목록(원본 ms 포함) → UI 선택지로 사용
bat_times = bat["battery_time_raw"].tolist()
if len(bat_times) < 2:
    st.error("배터리 데이터가 너무 적습니다.")
    st.stop()


def fmt_ts(ts: pd.Timestamp) -> str:
    # 밀리초 3자리까지 표시
    return ts.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]


bat_labels = [fmt_ts(t) for t in bat_times]


st.subheader("1단계) 배터리 기준 시간 범위 + 센서/스무딩 선택")


with st.form("main_form"):
    colA, colB = st.columns(2)
    with colA:
        start_idx = st.selectbox(
            "시작 시간(배터리 원본 시간)",
            options=list(range(len(bat_times))),
            format_func=lambda i: bat_labels[i],
            index=0
        )
    with colB:
        end_idx = st.selectbox(
            "끝 시간(배터리 원본 시간)",
            options=list(range(len(bat_times))),
            format_func=lambda i: bat_labels[i],
            index=len(bat_times) - 1
        )


    st.markdown("#### 센서 선택 (채널 1에서 4 / 센서 1에서 3 고정)")
    c1, c2 = st.columns(2)


    with c1:
        temp_ch = st.selectbox("온도 센서 채널", [1, 2, 3, 4], index=1)
        temp_si = st.selectbox("온도 센서 번호", [1, 2, 3], index=0)


    with c2:
        vol_ch = st.selectbox("부피 센서 채널", [1, 2, 3, 4], index=3)
        vol_si = st.selectbox("부피 센서 번호", [1, 2, 3], index=0)


    temp_col = f"CH{temp_ch}_{temp_si}"
    vol_col = f"CH{vol_ch}_{vol_si}"


    st.markdown("#### 스무딩(Forward) 설정")
    window_size = st.number_input("스무딩 윈도우(행 수)", min_value=1, value=39, step=1)


    run = st.form_submit_button("2단계) 처리 실행")


# ✅ 최초 실행 전에는 멈추되, 한 번 실행한 뒤에는 rerun되어도 유지
if not run and "final_df" not in st.session_state:
    st.stop()



# ---------------------------
# (1) 배터리 구간 슬라이스 (index 기준)
# ---------------------------
if end_idx < start_idx:
    st.error("끝 시간이 시작 시간보다 빠릅니다.")
    st.stop()


bat_slice = bat.iloc[start_idx:end_idx + 1].reset_index(drop=True)
N = len(bat_slice)
if N < 2:
    st.error("선택 구간의 배터리 데이터가 너무 적습니다.")
    st.stop()


start_bat_time = bat_slice["battery_time_raw"].iloc[0]


# ---------------------------
# (2) FBG 시작점을 nearest로 1회 결정 후 N행 슬라이스
# ---------------------------
fbg_start_i = find_nearest_index(fbg["fbg_time_raw"], start_bat_time)


fbg_slice = fbg.iloc[fbg_start_i:fbg_start_i + N].reset_index(drop=True)
if len(fbg_slice) < N:
    st.error(
        f"FBG 데이터가 부족합니다. (필요 {N}행, 실제 {len(fbg_slice)}행)\n"
        "→ 배터리 구간을 줄이거나, FBG 파일 범위를 확인하세요."
    )
    st.stop()


# ---------------------------
# (3) 행 기준 결합
# ---------------------------
merged = pd.concat(
    [bat_slice[["battery_time_raw", "Current_mA", "Voltage_V"]],
     fbg_slice[["fbg_time_raw", temp_col, vol_col]]],
    axis=1
)


# 센서 유효성 체크(구간 내 값이 실제 존재하는지)
if merged[temp_col].isna().all():
    st.error(f"선택한 온도 센서({temp_col})가 이 구간에 존재하지 않습니다(전부 NaN).")
    st.stop()
if merged[vol_col].isna().all():
    st.error(f"선택한 부피 센서({vol_col})가 이 구간에 존재하지 않습니다(전부 NaN).")
    st.stop()


# ---------------------------
# (4) 계산 + 스무딩
# ---------------------------
calc = compute_T_S(merged, temp_col=temp_col, vol_col=vol_col)
calc["T_smooth"] = forward_rolling_mean(calc["T_raw"], window_size=int(window_size))
calc["S_smooth"] = forward_rolling_mean(calc["S_raw"], window_size=int(window_size))



# ---------------------------
# (5) 미리보기/다운로드
# ---------------------------
st.subheader("처리 결과 미리보기")
st.caption(st.session_state.get("meta_caption", ""))

calc_to_show = st.session_state.get("calc_df", calc)
final = st.session_state.get("final_df", build_final_output(calc_to_show))

st.dataframe(calc_to_show.head(50), use_container_width=True)


# ✅ rerun되어도 미리보기/다운로드 유지용
st.session_state["calc_df"] = calc
st.session_state["final_df"] = final
st.session_state["meta_caption"] = (
    f"배터리 시작: {fmt_ts(bat_slice['battery_time_raw'].iloc[0])} / "
    f"FBG 시작(nearest): {fmt_ts(fbg_slice['fbg_time_raw'].iloc[0])} / "
    f"N={N}행"
)

st.subheader("다운로드")
final_to_download = st.session_state.get("final_df", final)

xbuf = io.BytesIO()
with pd.ExcelWriter(xbuf, engine="xlsxwriter") as writer:
    final_to_download.to_excel(writer, index=False, sheet_name="Result")

st.download_button(
    "결과 Excel 다운로드(.xlsx)",
    data=xbuf.getvalue(),
    file_name="FBG_Battery_Synced_Result.xlsx",
    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
)

st.download_button(
    "결과 CSV 다운로드(.csv)",
    data=final_to_download.to_csv(index=False).encode("utf-8-sig"),
    file_name="FBG_Battery_Synced_Result.csv",
    mime="text/csv",
)




