import streamlit as st
import pandas as pd
from io import StringIO, BytesIO
from processor import procesar_global, clean_cols

# =====================================================
# 🔧 CONFIGURACIÓN DE PÁGINA
# =====================================================
st.set_page_config(page_title="CLAIRPORT – Consolidado Global", layout="wide")
st.title("📊 Consolidado Global Aeroportuario – CLAIRPORT")

# =====================================================
# 📥 FUNCIONES DE LECTURA
# =====================================================

def read_generic_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)
    text = raw.decode("latin-1").replace("ï»¿","").replace("\ufeff","")
    sep = ";" if text.count(";") > text.count(",") else ","
    return pd.read_csv(StringIO(text), sep=sep, engine="python")

def read_auditorias_csv(uploaded_file):
    raw = uploaded_file.read()
    uploaded_file.seek(0)
    text = raw.decode("latin-1").replace("ï»¿","").replace("\ufeff","")
    return pd.read_csv(StringIO(text), sep=";", engine="python")

# =====================================================
# 📁 CARGA DE ARCHIVOS
# =====================================================

st.header("📥 Cargar Archivos – Todos obligatorios")

col1, col2 = st.columns(2)

with col1:
    ventas_file = st.file_uploader("🔵 Ventas (.csv / .xlsx)", type=["csv","xlsx"])
    perf_file = st.file_uploader("🟢 Performance (.csv)", type=["csv"])
    auditorias_file = st.file_uploader("🟣 Auditorías (.csv)", type=["csv"])
    offtime_file = st.file_uploader("🟠 Off-Time (.csv)", type=["csv"])

with col2:
    dur90_file = st.file_uploader("🔴 >90 min (.csv)", type=["csv"])
    dur30_file = st.file_uploader("🟤 >30 min (.csv)", type=["csv"])
    inspecciones_file = st.file_uploader("🚗 Inspecciones (.xlsx)", type=["xlsx"])
    abandonados_file = st.file_uploader("👥 Abandonados (.xlsx)", type=["xlsx"])
    rescates_file = st.file_uploader("🆘 Rescates (.csv)", type=["csv"])
    whatsapp_file = st.file_uploader("💬 WhatsApp (.csv)", type=["csv"])

# =====================================================
# 📅 FECHAS
# =====================================================

st.header("📅 Seleccionar Rango de Fechas")

c1, c2 = st.columns(2)
with c1:
    date_from = st.date_input("Desde:", format="YYYY-MM-DD")
with c2:
    date_to = st.date_input("Hasta:", format="YYYY-MM-DD")

if not date_from or not date_to:
    st.stop()

date_from = pd.to_datetime(date_from)
date_to = pd.to_datetime(date_to)

st.divider()

# =====================================================
# 🚀 BOTÓN PROCESAR
# =====================================================

if st.button("🚀 Procesar Consolidado Global", type="primary"):

    # Validar carga
    required = [
        ventas_file, perf_file, auditorias_file, offtime_file,
        dur90_file, dur30_file, inspecciones_file,
        abandonados_file, rescates_file, whatsapp_file
    ]

    if not all(required):
        st.error("❌ Debes cargar TODOS los archivos antes de procesar.")
        st.stop()

    # --- LECTURA ---
    try:
        df_ventas = pd.read_excel(ventas_file) if ventas_file.name.endswith(".xlsx") else read_generic_csv(ventas_file)
        df_perf = read_generic_csv(perf_file)
        df_aud = read_auditorias_csv(auditorias_file)
        df_off = read_generic_csv(offtime_file)
        df_dur90 = read_generic_csv(dur90_file)
        df_dur30 = read_generic_csv(dur30_file)
        df_ins = pd.read_excel(inspecciones_file)
        df_aband = pd.read_excel(abandonados_file)
        df_resc = read_generic_csv(rescates_file)
        df_wa = read_generic_csv(whatsapp_file)

    except Exception as e:
        st.error(f"❌ Error leyendo archivos: {e}")
        st.stop()

    # --- PROCESAMIENTO ---
    try:
        df_diario, df_sem, df_periodo, df_transp = procesar_global(
            df_ventas, df_perf, df_aud, df_off,
            df_dur90, df_dur30, df_ins,
            df_aband, df_resc, df_wa,
            date_from, date_to
        )
        
        st.success("✅ Consolidado generado con éxito")

        st.subheader("📅 Diario")
        st.dataframe(df_diario)

        st.subheader("📆 Semanal")
        st.dataframe(df_sem)

        st.subheader("📊 Periodo")
        st.dataframe(df_periodo)

        st.subheader("📐 Vista Traspuesta")
        st.dataframe(df_transp)

        # DESCARGA
        output = BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_diario.to_excel(writer, index=False, sheet_name="Diario")
            df_sem.to_excel(writer, index=False, sheet_name="Semanal")
            df_periodo.to_excel(writer, index=False, sheet_name="Periodo")
            df_transp.to_excel(writer, index=False, sheet_name="Vista_Traspuesta")

            workbook = writer.book
            ws = writer.sheets["Vista_Traspuesta"]
            purple = workbook.add_format({"bg_color": "#4A2B8D", "font_color": "white", "bold": True})

            for i, col in enumerate(df_transp.columns):
                if isinstance(col, str) and col.startswith("Semana "):
                    ws.set_column(i, i, 22, purple)

        st.download_button(
            "💾 Descargar Excel",
            data=output.getvalue(),
            file_name="Consolidado_Global.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        )

    except Exception as e:
        st.error(f"❌ Error procesando datos: {e}")
        
        # === DIAGNÓSTICO DE ERRORES ===
        st.divider()
        st.warning("⚠️ Se detectó un error. Despliega el diagnóstico abajo para encontrar la causa.")
        
        with st.expander("🔍 DIAGNÓSTICO DE COLUMNAS (Clic para abrir)"):
            st.write("El sistema verificará las columnas numéricas críticas.")
            
            # Chequeo Performance
            st.write("**1. Verificando Performance:**")
            cols_check = ["CSAT", "NPS Score", "Firt (h)", "firt_pct", "Furt (h)", "furt_pct"]
            df_p_clean = clean_cols(df_perf.copy())
            df_p_clean = df_p_clean.rename(columns={"% Firt": "firt_pct", "% Furt": "furt_pct"})
            
            for c in cols_check:
                if c in df_p_clean.columns:
                    # Intentar conversión
                    try:
                        temp = (df_p_clean[c].astype(str)
                                .str.replace(",", ".", regex=False)
                                .str.replace("%", "", regex=False)
                                .str.strip())
                        pd.to_numeric(temp)
                        st.caption(f"✅ Columna `{c}`: OK (convertible a número)")
                    except Exception as err_col:
                        st.error(f"❌ Columna `{c}`: FALLO. Contiene valores no numéricos que impiden el promedio. ({err_col})")
                        st.write("Muestra de valores únicos:", df_p_clean[c].unique()[:10])
                else:
                    st.caption(f"⚠️ Columna `{c}` no encontrada (no es error crítico si no existe).")

            # Chequeo Auditorías
            st.write("**2. Verificando Auditorías:**")
            df_a_clean = clean_cols(df_aud.copy())
            if "Total Audit Score" in df_a_clean.columns:
                try:
                    temp_a = (df_a_clean["Total Audit Score"].astype(str)
                              .str.replace(",", ".", regex=False)
                              .str.replace("%", "", regex=False)
                              .str.strip())
                    pd.to_numeric(temp_a)
                    st.caption("✅ Columna `Total Audit Score`: OK")
                except:
                    st.error("❌ Columna `Total Audit Score`: FALLO. No es numérica.")
                    st.write("Muestra:", df_a_clean["Total Audit Score"].unique()[:5])
            else:
                st.caption("⚠️ No se encontró la columna 'Total Audit Score'.")
            
            st.info("Si ves una ❌ arriba, ese es el archivo que debes corregir en el Excel origen.")
