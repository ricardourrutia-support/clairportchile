# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

# ============================================================
# 🔧 LIMPIEZA DE COLUMNAS
# ============================================================

def clean_cols(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = (
        df.columns.astype(str)
        .str.replace("ï»¿", "", regex=False)
        .str.replace("\ufeff", "", regex=False)
        .str.strip()
    )
    return df

def safe_pct(numer: pd.Series, denom: pd.Series) -> pd.Series:
    denom2 = denom.replace(0, np.nan)
    return (100.0 * numer / denom2)

# ============================================================
# 🟦 PROCESAR VENTAS
# ============================================================

def process_ventas(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)

    if "tm_start_local_at" in df.columns:
        df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()
    elif "createdAt_local" in df.columns:
        df["fecha"] = pd.to_datetime(df["createdAt_local"], errors="coerce").dt.normalize()
    elif "date" in df.columns:
        df["fecha"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True).dt.normalize()
    else:
        return pd.DataFrame(columns=[
            "fecha", "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas",
            "Q_journeys", "Q_pasajeros", "Q_pasajeros_exclusives", "Q_pasajeros_compartidas"
        ])

    if "qt_price_local" in df.columns:
        df["qt_price_local"] = (
            df["qt_price_local"]
            .astype(str)
            .str.replace(",", "", regex=False)
            .str.replace(" ", "", regex=False)
            .str.replace("$", "", regex=False)
        )
        df["qt_price_local"] = pd.to_numeric(df["qt_price_local"], errors="coerce")
    else:
        df["qt_price_local"] = np.nan

    prod = df.get("ds_product_name", pd.Series([""] * len(df), index=df.index)).astype(str).str.lower().str.strip()

    df["Ventas_Totales"] = df["qt_price_local"]
    df["Ventas_Compartidas"] = np.where(prod == "van_compartida", df["qt_price_local"], 0)
    df["Ventas_Exclusivas"] = np.where(prod == "van_exclusive", df["qt_price_local"], 0)

    fr_col = None
    for c in ["finishReason", "finisReason", "FinishReason", "finish_reason", "Finish Reason"]:
        if c in df.columns:
            fr_col = c
            break

    if fr_col is None:
        is_dropoff = pd.Series([False] * len(df), index=df.index)
    else:
        is_dropoff = df[fr_col].astype(str).str.strip().str.upper().eq("FINISH_REASON_DROPOFF")

    df["Q_pasajeros"] = is_dropoff.astype(int)
    df["Q_pasajeros_exclusives"] = np.where(is_dropoff & (prod == "van_exclusive"), 1, 0)
    df["Q_pasajeros_compartidas"] = np.where(is_dropoff & (prod == "van_compartida"), 1, 0)

    if "journey_id" in df.columns:
        jid = df["journey_id"].astype(str).str.strip()
        df["_jid"] = jid
    else:
        df["_jid"] = ""

    diario = df.groupby("fecha", as_index=False).agg({
        "Ventas_Totales": "sum",
        "Ventas_Compartidas": "sum",
        "Ventas_Exclusivas": "sum",
        "Q_pasajeros": "sum",
        "Q_pasajeros_exclusives": "sum",
        "Q_pasajeros_compartidas": "sum",
    })

    if "journey_id" in df.columns:
        qj = (
            df[is_dropoff & df["_jid"].ne("") & df["_jid"].notna()]
            .groupby("fecha")["_jid"]
            .nunique()
            .reset_index()
            .rename(columns={"_jid": "Q_journeys"})
        )
        diario = diario.merge(qj, on="fecha", how="left")
    else:
        diario["Q_journeys"] = 0

    diario["Q_journeys"] = diario["Q_journeys"].fillna(0)
    return diario

# ============================================================
# 🟩 PROCESAR PERFORMANCE
# ============================================================

def process_performance(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df = df.rename(columns={"% Firt": "firt_pct", "% Furt": "furt_pct"})

    cols_to_force_numeric = ["CSAT", "NPS Score", "Firt (h)", "firt_pct", "Furt (h)", "furt_pct"]
    for col in cols_to_force_numeric:
        if col in df.columns:
            df[col] = (
                df[col].astype(str)
                .str.replace("%", "", regex=False)
                .str.replace(",", ".", regex=False)
                .str.strip()
            )
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df["fecha"] = pd.to_datetime(df["Fecha de Referencia"], errors="coerce").dt.normalize()
    df["Q_Ticket"] = 1
    
    status = df["Status"].astype(str).str.lower().str.strip()
    df["Q_Tickets_Resueltos"] = np.where(status != "pending", 1, 0)

    df["Q_Encuestas"] = np.where(df["CSAT"].notna() | df["NPS Score"].notna(), 1, 0)

    diario = df.groupby("fecha", as_index=False).agg({
        "Q_Encuestas": "sum",
        "CSAT": "mean",
        "NPS Score": "mean",
        "Firt (h)": "mean",
        "firt_pct": "mean",
        "Furt (h)": "mean",
        "furt_pct": "mean",
        "Reopen": "sum",
        "Q_Ticket": "sum",
        "Q_Tickets_Resueltos": "sum"
    })

    return diario

# ============================================================
# 🟪 PROCESAR AUDITORÍAS
# ============================================================

def process_auditorias(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    candidates = ["Date Time Reference", "Date Time", "ï»¿Date Time"]
    col_fecha = next((c for c in candidates if c in df.columns), None)

    if col_fecha is None:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    def to_date_aud(x):
        if pd.isna(x): return None
        if isinstance(x, (int, float)):
            try:
                if x > 30000:
                    return (datetime(1899, 12, 30) + timedelta(days=float(x))).date()
            except: pass
        s = str(x).strip()
        for fmt in ("%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%d-%m-%y", "%Y-%m-%d", "%Y/%m/%d"):
            try: return datetime.strptime(s, fmt).date()
            except: pass
        try: return pd.to_datetime(s, dayfirst=True).date()
        except: return None

    df["fecha"] = df[col_fecha].apply(to_date_aud)
    df = df[df["fecha"].notna()]
    df["fecha"] = pd.to_datetime(df["fecha"])

    if "Total Audit Score" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Q_Auditorias", "Nota_Auditorias"])

    score_raw = (
        df["Total Audit Score"]
        .astype(str)
        .str.replace(",", ".", regex=False)
        .str.replace("%", "", regex=False)
        .str.strip()
    )
    df["Nota_Auditorias"] = pd.to_numeric(score_raw, errors="coerce").fillna(0)
    df["Q_Auditorias"] = 1

    diario = df.groupby("fecha", as_index=False).agg({
        "Q_Auditorias": "sum",
        "Nota_Auditorias": "mean"
    })

    return diario

# ============================================================
# 🟧 OTROS PROCESADORES
# ============================================================

def process_offtime(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["tm_start_local_at"], errors="coerce").dt.normalize()
    df["OFF_TIME"] = np.where(
        df["Segment Arrived to Airport vs Requested"] != "02. A tiempo (0-20 min antes)",
        1, 0
    )
    return df.groupby("fecha", as_index=False).agg({"OFF_TIME": "sum"})

def process_duracion(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Start At Local Dt"], errors="coerce").dt.normalize()
    df["Duracion_90"] = np.where(pd.to_numeric(df["Duration (Minutes)"], errors="coerce") > 90, 1, 0)
    return df.groupby("fecha", as_index=False).agg({"Duracion_90": "sum"})

def process_duracion30(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    col_name = "Day of tm_start_local_at"
    
    if col_name not in df.columns:
        return pd.DataFrame(columns=["fecha", "Duracion_30"])

    def parse_english_date(val):
        s = str(val).strip()
        meses = {
            "January": "01", "February": "02", "March": "03", "April": "04", "May": "05", "June": "06",
            "July": "07", "August": "08", "September": "09", "October": "10", "November": "11", "December": "12"
        }
        for m_eng, m_num in meses.items():
            if m_eng in s:
                s = s.replace(m_eng, m_num)
                break
        try: return datetime.strptime(s, "%m %d, %Y")
        except: return pd.NaT

    df["fecha_temp"] = pd.to_datetime(df[col_name], errors="coerce")
    mask_nat = df["fecha_temp"].isna()
    if mask_nat.any():
        df.loc[mask_nat, "fecha_temp"] = df.loc[mask_nat, col_name].apply(parse_english_date)

    df["fecha"] = pd.to_datetime(df["fecha_temp"]).dt.normalize()
    df = df[df["fecha"].notna()]
    
    df["Duracion_30"] = 1
    return df.groupby("fecha", as_index=False).agg({"Duracion_30": "sum"})

def process_inspecciones(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.normalize()

    for c in ["Cumplimiento Exterior", "Cumplimiento Interior", "Cumplimiento Conductor"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df["Inspecciones_Q"] = 1
    df["Cump_Exterior"] = (df["Cumplimiento Exterior"] == 100).astype(int)
    df["Incump_Exterior"] = ((df["Cumplimiento Exterior"] < 100) & df["Cumplimiento Exterior"].notna()).astype(int)
    df["Cump_Interior"] = (df["Cumplimiento Interior"] == 100).astype(int)
    df["Incump_Interior"] = ((df["Cumplimiento Interior"] < 100) & df["Cumplimiento Interior"].notna()).astype(int)
    df["Cump_Conductor"] = (df["Cumplimiento Conductor"] == 100).astype(int)
    df["Incump_Conductor"] = ((df["Cumplimiento Conductor"] < 100) & df["Cumplimiento Conductor"].notna()).astype(int)

    diario = df.groupby("fecha", as_index=False).agg({
        "Inspecciones_Q": "sum", "Cump_Exterior": "sum", "Incump_Exterior": "sum",
        "Cump_Interior": "sum", "Incump_Interior": "sum", "Cump_Conductor": "sum", "Incump_Conductor": "sum",
    })
    return diario

def process_abandonados(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    df["fecha"] = pd.to_datetime(df["Marca temporal"], errors="coerce").dt.normalize()
    df["Abandonados"] = 1
    return df.groupby("fecha", as_index=False).agg({"Abandonados": "sum"})

def process_rescates(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    col_fecha = "Start At Local Dt"
    if col_fecha not in df.columns:
        return pd.DataFrame(columns=["fecha", "Rescates"])

    df["fecha"] = pd.to_datetime(df[col_fecha], dayfirst=True, errors="coerce").dt.normalize()
    df = df[df["fecha"].notna()]
    df["Rescates"] = 1
    return df.groupby("fecha", as_index=False).agg({"Rescates": "sum"})

def process_whatsapp(df: pd.DataFrame) -> pd.DataFrame:
    df = clean_cols(df)
    if "Created At Local Dt" not in df.columns:
        return pd.DataFrame(columns=["fecha", "Q_Tickets_WA"])
    df["fecha"] = pd.to_datetime(df["Created At Local Dt"], errors="coerce").dt.normalize()
    df["Q_Tickets_WA"] = 1
    return df.groupby("fecha", as_index=False).agg({"Q_Tickets_WA": "sum"})

# ============================================================
# 📅 SEMANA HUMANA
# ============================================================

def semana_humana(fecha: pd.Timestamp) -> str:
    lunes = fecha - pd.Timedelta(days=fecha.weekday())
    domingo = lunes + pd.Timedelta(days=6)
    meses = {
        1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
        7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
    }
    return f"{lunes.day}-{domingo.day} {meses[domingo.month]}"

# ============================================================
# 🔵 PROCESAR GLOBAL
# ============================================================

def procesar_global(df_ventas, df_perf, df_aud, df_off, df_dur, df_dur30, df_insp, df_aband, df_resc, df_whatsapp, date_from, date_to):
    v = process_ventas(df_ventas)
    p = process_performance(df_perf)
    a = process_auditorias(df_aud)
    o = process_offtime(df_off)
    d = process_duracion(df_dur)
    d30 = process_duracion30(df_dur30)
    insp = process_inspecciones(df_insp)
    ab = process_abandonados(df_aband)
    resc = process_rescates(df_resc)
    wa = process_whatsapp(df_whatsapp)

    df = (v.merge(p, on="fecha", how="outer").merge(a, on="fecha", how="outer")
           .merge(o, on="fecha", how="outer").merge(d, on="fecha", how="outer")
           .merge(d30, on="fecha", how="outer").merge(insp, on="fecha", how="outer")
           .merge(ab, on="fecha", how="outer").merge(resc, on="fecha", how="outer")
           .merge(wa, on="fecha", how="outer"))

    df = df[(df["fecha"] >= date_from) & (df["fecha"] <= date_to)].sort_values("fecha")

    sum_cols = ["Q_Encuestas", "Reopen", "Q_Ticket", "Q_Tickets_Resueltos", "Q_Tickets_WA", "Q_Auditorias",
                "Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas", "Q_journeys", "Q_pasajeros", 
                "Q_pasajeros_exclusives", "Q_pasajeros_compartidas", "OFF_TIME", "Duracion_90", "Duracion_30", 
                "Abandonados", "Rescates", "Inspecciones_Q", "Cump_Exterior", "Incump_Exterior", "Cump_Interior", 
                "Incump_Interior", "Cump_Conductor", "Incump_Conductor"]

    mean_cols = ["CSAT", "NPS Score", "Firt (h)", "Furt (h)", "firt_pct", "furt_pct", "Nota_Auditorias"]

    for c in sum_cols:
        if c in df.columns:
            df[c] = df[c].fillna(0)

    for c in mean_cols:
        if c in df.columns and c != "Nota_Auditorias":
            df[c] = df[c].replace({0: np.nan})

    operativos = ["OFF_TIME", "Duracion_90", "Duracion_30", "Abandonados", "Rescates"]
    pct_cols = []
    for op in operativos:
        colp = f"{op}_pct_pasajeros"
        df[colp] = safe_pct(df[op], df["Q_pasajeros"]).round(4)
        pct_cols.append(colp)

    df_sem = df.copy()
    df_sem["Semana"] = df_sem["fecha"].apply(semana_humana)
    agg = {c: "sum" for c in sum_cols}
    agg.update({c: "mean" for c in mean_cols})
    df_sem = df_sem.groupby("Semana", as_index=False).agg(agg)
    for op in operativos:
        df_sem[f"{op}_pct_pasajeros"] = safe_pct(df_sem[op], df_sem["Q_pasajeros"]).round(4)

    df_per = df.copy()
    df_per["Periodo"] = f"{date_from.date()} → {date_to.date()}"
    df_per = df_per.groupby("Periodo", as_index=False).agg(agg)
    for op in operativos:
        df_per[f"{op}_pct_pasajeros"] = safe_pct(df_per[op], df_per["Q_pasajeros"]).round(4)

    df_transp = build_transposed_view(df, sum_cols=sum_cols, mean_cols=mean_cols, pct_cols=pct_cols)

    return df, df_sem, df_per, df_transp

def build_transposed_view(df_diario, sum_cols, mean_cols, pct_cols=None):
    if df_diario is None or df_diario.empty: return pd.DataFrame()
    df = df_diario.copy()
    df["fecha"] = pd.to_datetime(df["fecha"]).dt.normalize()
    df = df.sort_values("fecha")
    kpis = [c for c in df.columns if c != "fecha"]
    
    operativos = ["OFF_TIME", "Duracion_90", "Duracion_30", "Abandonados", "Rescates"]
    if pct_cols is None: pct_cols = [f"{op}_pct_pasajeros" for op in operativos if f"{op}_pct_pasajeros" in df.columns]
    
    meses = {1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"}
    def week_label(s_date, e_date): return f"Semana {s_date.day:02d} al {e_date.day:02d} {meses[e_date.month]} {e_date.year}"
    def month_label(a_date): return f"Mes {meses[a_date.month]} {a_date.year}"
    def recompute_pct(subdf, op_name):
        denom = subdf.get("Q_pasajeros", pd.Series([0]*len(subdf), index=subdf.index)).sum()
        if denom == 0: return np.nan
        return (subdf[op_name].sum() / denom) * 100.0

    all_dates = [pd.to_datetime(d).normalize() for d in sorted(df["fecha"].unique())]
    result = pd.DataFrame(index=kpis)

    for i, d in enumerate(all_dates):
        day_df = df[df["fecha"] == d]
        col_day = d.strftime("%d/%m/%Y")
        result[col_day] = day_df[kpis].iloc[0] if len(day_df) > 0 else np.nan

        if d.weekday() == 6:
            ws = d - pd.Timedelta(days=6)
            week_df = df[(df["fecha"] >= ws) & (df["fecha"] <= d)]
            vals = []
            for k in kpis:
                if k in pct_cols: vals.append(recompute_pct(week_df, k.replace("_pct_pasajeros", "")))
                elif k in sum_cols: vals.append(week_df[k].sum())
                elif k in mean_cols: vals.append(week_df[k].mean())
                else: vals.append(np.nan)
            result[week_label(ws, d)] = vals

        next_d = all_dates[i + 1] if i + 1 < len(all_dates) else None
        if (next_d is None) or (next_d.month != d.month) or (next_d.year != d.year):
            ms = d.replace(day=1)
            month_df = df[(df["fecha"] >= ms) & (df["fecha"] <= d)]
            vals = []
            for k in kpis:
                if k in pct_cols: vals.append(recompute_pct(month_df, k.replace("_pct_pasajeros", "")))
                elif k in sum_cols: vals.append(month_df[k].sum())
                elif k in mean_cols: vals.append(month_df[k].mean())
                else: vals.append(np.nan)
            result[month_label(d)] = vals

    grupos = {
        "VENTAS (MONTO)": ["Ventas_Totales", "Ventas_Compartidas", "Ventas_Exclusivas"],
        "VENTAS (VOLUMEN)": ["Q_journeys", "Q_pasajeros", "Q_pasajeros_exclusives", "Q_pasajeros_compartidas"],
        "PERFORMANCE": ["Q_Ticket", "Q_Tickets_WA", "Q_Tickets_Resueltos", "Reopen"],
        "CALIDAD (ENCUESTAS & SLA)": ["Q_Encuestas", "CSAT", "NPS Score", "Firt (h)", "firt_pct", "Furt (h)", "furt_pct", "Q_Auditorias", "Nota_Auditorias"],
        "INSPECCIONES": ["Inspecciones_Q", "Cump_Exterior", "Incump_Exterior", "Cump_Interior", "Incump_Interior", "Cump_Conductor", "Incump_Conductor"],
        "OTROS (OPERATIVOS)": ["OFF_TIME", "OFF_TIME_pct_pasajeros", "Duracion_90", "Duracion_90_pct_pasajeros", "Duracion_30", "Duracion_30_pct_pasajeros", "Abandonados", "Abandonados_pct_pasajeros", "Rescates", "Rescates_pct_pasajeros"]
    }

    k_present = list(result.index)
    used = set()
    new_index = []

    for gr, lista in grupos.items():
        pres = [k for k in lista if k in k_present]
        if pres:
            new_index.append(f"=== {gr} ===")
            new_index.extend(pres)
            used.update(pres)

    restantes = [k for k in k_present if k not in used]
    if restantes:
        new_index.append("=== OTROS KPI ===")
        new_index.extend(restantes)

    result = result.reindex(new_index)
    result.insert(0, "KPI", result.index)
    return result.reset_index(drop=True)
