import io
import zipfile
import tempfile
import os
import streamlit as st

try:
    import rarfile
    import sys
    if sys.platform == "win32":
        rarfile.UNRAR_TOOL = r"C:\Program Files\WinRAR\UnRAR.exe"
    # En Linux (Streamlit Cloud) usa el unrar del sistema (instalado via packages.txt)
    _RAR_OK = True
except ImportError:
    _RAR_OK = False
import pandas as pd
import numpy as np
import requests
import urllib3
import psycopg2
import psycopg2.extras

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ════════════════════════════════════════════════════════════════════════════════
# CONFIGURACIÓN
# ════════════════════════════════════════════════════════════════════════════════

# Credenciales Supabase
# En producción (Streamlit Cloud) se leen desde st.secrets
def _pg_config() -> dict:
    try:
        pwd = st.secrets["supabase"]["password"]
    except Exception:
        pwd = "MoJo_JoJo26#@"   # fallback local
    return dict(
        host     = "db.pnidiixppefxfiqwzvwz.supabase.co",
        port     = 5432,
        dbname   = "postgres",
        user     = "postgres",
        password = pwd,
        sslmode  = "require",
    )

MES_TXT = [
    "ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO",
    "JULIO","AGOSTO","SETIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE",
]

FECHA_INICIO_INICIAL = pd.Timestamp("2024-12-01")

MOTIVOS_SHEET    = "MOTIVOS_RDO"
COSTOS_SHEET     = "COSTO_RDO"
INDICES_SHEET    = "INDICES_RDO"
GENERACION_SHEET = "GENERACION_RDO"

ARCHIVOS_DEMANDA = {
    "Hidro - Despacho (MW).csv":         "HIDRO",
    "Termica - Despacho (MW).csv":       "TERMICA",
    "Rer y No COES - Despacho (MW).csv": "RER",
}
ARCHIVO_HIDRO   = "Hidro - Despacho (MW).csv"
ARCHIVO_RER     = "Rer y No COES - Despacho (MW).csv"
ARCHIVO_TERMICA = "Termica - Despacho (MW).csv"

RDO_LETRAS_GENERACION = list("ABCDEFGH")

HORAS_GENERACION = [
    f"{((i + 1) * 30) // 60 % 24:02d}:{((i + 1) * 30) % 60:02d}"
    for i in range(48)
]

BARRAS_HIDRO = [
    "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR",
    "PURMACANA","NIMPERIAL","PIZARRAS","POECHOS2","POECHOS1",
    "CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII",
    "YANAPAMPA","POTRERO","CH MARANON","YARUCAYA","CHHER1",
    "CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","8/08/2026",
    "RENOVANDESH1","CH RENOVANDES","CH MANTA","SANTA ROSA 1",
    "SANTA ROSA 2","TUPURI","CH HUALLIN",
]
BARRAS_EOLICA = [
    "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA",
    "PQEEOLICO3HERMANAS","WAYRAI","HUAMBOS","DUNA",
    "CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
    "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP",
]
BARRAS_SOLAR = [
    "MAJES","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
    "CS RUBI","INTIPAMPA","CS INTIPAMPA EXPANSION",
    "CSEXPANSIONINTIPAMPA","YARUCAYA","CS YARUCAYA","CSF YARUCAYA",
    "CSCLEMESI","CS CARHUAQUERO","EL CARMEN","CS EL CARMEN",
    "CS MATARANI","CS SAN MARTIN","CSSUNNY","CSSUNNYEXP","LAGRINGAV",
    "CSSUNNYEXPANSION","CSCOENERGY",
]
BARRAS_TERMICA_RER = [
    "MAPLE","PARAMONGA","REPARTICION","HUAYCOLORO","TABLAZO",
    "CTB DONA CATALINA","CT CANA BRAVA","CT SAN JACINTO","CTB CALLAO",
    "CT TALLANCA","CTAGROOLMOS","CASAGRANDE","CSCOENERGY","PIAS",
]

# ════════════════════════════════════════════════════════════════════════════════
# URLS DEL PORTAL COES
# ════════════════════════════════════════════════════════════════════════════════

_BASE_PDO = (
    "https://www.coes.org.pe/portal/browser/download?"
    "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FPrograma%20Diario%2F"
    "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FYUPANA_{y}{m}{d}.zip"
)
_BASE_RDO = (
    "https://www.coes.org.pe/portal/browser/download?"
    "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
    "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}{letra}%2FYUPANA_{d}{m}{letra}.zip"
)
_BASE_MOTIVO = (
    "https://www.coes.org.pe/portal/browser/download?"
    "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
    "{y}%2F{m}_{M}%2FD%C3%ADa%20{dd}%2FReprog%20{dd}{mm}{L}%2FReprog_{dd}{mm}{L}.xlsx"
)
_BASE_INDICES = (
    "https://www.coes.org.pe/portal/browser/download?"
    "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
    "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{dd}{mm}{L}%2Findices{y}{mm}{dd}_{L}.xlsx"
)


def _mes(m: int) -> str:
    return MES_TXT[m - 1]

def _url_pdo(fecha):
    y, m, d = fecha.year, f"{fecha.month:02d}", f"{fecha.day:02d}"
    return _BASE_PDO.format(y=y, m=m, M=_mes(fecha.month), d=d)

def _url_rdo(fecha, letra):
    y, m, d = fecha.year, f"{fecha.month:02d}", f"{fecha.day:02d}"
    return _BASE_RDO.format(y=y, m=m, M=_mes(fecha.month), d=d, letra=letra)

def _url_motivo(fecha, letra):
    y, m, d = fecha.year, f"{fecha.month:02d}", f"{fecha.day:02d}"
    return _BASE_MOTIVO.format(y=y, m=m, M=_mes(fecha.month), d=d, dd=d, mm=m, L=letra)

def _url_indices(fecha, letra):
    y, m, d = fecha.year, f"{fecha.month:02d}", f"{fecha.day:02d}"
    return _BASE_INDICES.format(y=y, m=m, M=_mes(fecha.month), d=d, dd=d, mm=m, L=letra)


# ════════════════════════════════════════════════════════════════════════════════
# HTTP + CACHÉ DE ZIPs
# ════════════════════════════════════════════════════════════════════════════════

_zip_cache: dict = {}

def _get(url: str, timeout: int = 60):
    candidatos = list(dict.fromkeys([url, url.replace("SETIEMBRE", "SEPTIEMBRE")]))
    for u in candidatos:
        try:
            r = requests.get(u, timeout=timeout, verify=False)
            if r.status_code == 200 and r.content:
                return r.content
        except requests.RequestException:
            continue
    return None

def _abrir_zip(data: bytes):
    """Abre ZIP o RAR y devuelve dict {nombre: bytes}."""
    if not data:
        return None

    # — ZIP —
    if data[:4] == b"PK\x03\x04":
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                return {n: zf.read(n) for n in zf.namelist()}
        except Exception:
            return None

    # — RAR —
    if data[:7] == b"Rar!\x1a\x07\x00" or data[:8] == b"Rar!\x1a\x07\x01\x00":
        if not _RAR_OK:
            return None
        try:
            with tempfile.NamedTemporaryFile(suffix=".rar", delete=False) as tmp:
                tmp.write(data)
                tmp_path = tmp.name
            with rarfile.RarFile(tmp_path) as rf:
                resultado = {n: rf.read(n) for n in rf.namelist()}
            os.unlink(tmp_path)
            return resultado
        except Exception:
            try:
                os.unlink(tmp_path)
            except Exception:
                pass
            return None

    return None

def obtener_zip(fecha, tipo, letra=None):
    key = (fecha.date().isoformat(), tipo, letra)
    if key not in _zip_cache:
        url = _url_pdo(fecha) if tipo == "PDO" else _url_rdo(fecha, letra)
        _zip_cache[key] = _abrir_zip(_get(url)) or {}
    return _zip_cache[key]

def _buscar_en_zip(contenido, nombre):
    nl = nombre.lower()
    for path, data in contenido.items():
        p = path.replace("\\", "/")
        if p.lower().endswith("/" + nl) or p.lower() == nl:
            return data
    return None

def _leer_csv(data):
    if not data:
        return None
    for enc in ("utf-8-sig", "latin-1"):
        try:
            return pd.read_csv(io.BytesIO(data), sep=",", encoding=enc, engine="python")
        except Exception:
            pass
    return None


# ════════════════════════════════════════════════════════════════════════════════
# SUPABASE — CONEXIÓN Y HELPERS
# ════════════════════════════════════════════════════════════════════════════════

def _pg_con():
    return psycopg2.connect(**_pg_config())


def _max_fecha_pg(tabla: str):
    """SELECT MAX(Fecha) de una tabla en Supabase."""
    try:
        con = _pg_con()
        cur = con.cursor()
        cur.execute(f'SELECT MAX("Fecha") FROM "{tabla}"')
        val = cur.fetchone()[0]
        con.close()
        return pd.Timestamp(val) if val else None
    except Exception:
        return None


def _insertar_df_pg(df: pd.DataFrame, tabla: str, con) -> int:
    """
    Para cada fecha del df:
      - Si ya existe en la tabla → borra esas filas y las reinserta (refresco)
      - Si no existe             → inserta directamente
    Así el último día guardado siempre queda completo aunque antes estuviera desfasado.
    Retorna el número de filas insertadas.
    """
    if df is None or df.empty:
        return 0
    df = df.copy()
    if "Fecha" in df.columns:
        df["Fecha"] = pd.to_datetime(df["Fecha"], errors="coerce").dt.strftime("%Y-%m-%d")

    cur = con.cursor()
    try:
        # Borrar filas de las fechas que vamos a (re)insertar
        fechas_a_insertar = tuple(df["Fecha"].dropna().unique().tolist())
        if fechas_a_insertar:
            cur.execute(
                f'DELETE FROM "{tabla}" WHERE "Fecha" = ANY(%s)',
                (list(fechas_a_insertar),)
            )
    except Exception:
        pass  # tabla no existe aún → insertar todo sin borrar

    if df.empty:
        return 0

    # Reemplazar NaN con None para que psycopg2 lo convierta a NULL
    df = df.where(pd.notnull(df), None)

    cols = list(df.columns)
    cols_quoted = [f'"{c}"' for c in cols]
    placeholders = ", ".join(["%s"] * len(cols))
    sql = f'INSERT INTO "{tabla}" ({", ".join(cols_quoted)}) VALUES ({placeholders})'

    rows = [tuple(r) for r in df.itertuples(index=False)]
    psycopg2.extras.execute_batch(cur, sql, rows, page_size=500)
    con.commit()
    return len(df)


# ════════════════════════════════════════════════════════════════════════════════
# UTILIDADES COMPARTIDAS
# ════════════════════════════════════════════════════════════════════════════════

def fila_sin_primer_valor(df):
    if df is None or df.empty:
        return None
    if df.shape[1] > 1:
        return df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce").sum(axis=1).tolist()
    total = []
    for celda in df.iloc[:, 0].astype(str):
        nums = [float(x) for x in celda.split(",")[1:] if x.strip()]
        total.append(sum(nums))
    return total

def rellenar_hasta_48(lst):
    if not lst:
        return None
    faltan = 48 - len(lst)
    return ([0] * faltan + lst) if faltan > 0 else lst[:48]


# ════════════════════════════════════════════════════════════════════════════════
# TAB 1 — DEMANDA
# ════════════════════════════════════════════════════════════════════════════════

def _procesar_csv_de_zip(fecha, tipo, letra, nombre_csv, fuente):
    contenido = obtener_zip(fecha, tipo, letra)
    data = _buscar_en_zip(contenido, nombre_csv)
    if data is None:
        return pd.DataFrame()
    df = _leer_csv(data)
    valores = fila_sin_primer_valor(df)
    if not valores:
        return pd.DataFrame()
    valores_48 = rellenar_hasta_48(valores)
    if valores_48 is None:
        return pd.DataFrame()
    anio    = fecha.year
    fecha_d = str(fecha.date())
    if tipo == "PDO":
        path    = f"PDO/{anio}/{fecha_d}/{nombre_csv}"
        rdo_val = None
    else:
        path    = f"RDO/{anio}/{fecha_d}/{letra}/{nombre_csv}"
        rdo_val = letra          # ← solo la letra (A, B…), sin "RDO " al inicio
    return pd.DataFrame({
        "Año":       anio,
        "Fecha":     fecha_d,
        "TipoSerie": tipo,
        "RDO":       rdo_val,
        "Archivo":   nombre_csv,
        "Periodo":   range(1, 49),
        "MW":        valores_48,
        "Fuente":    fuente,
        "path":      path,
    })


def procesar_demanda_rango(fecha_inicio, fecha_fin, log):
    fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
    total  = len(fechas)
    tablas = []
    log.write(f"DEMANDA — {fecha_inicio.date()} → {fecha_fin.date()} ({total} días)")
    prog = st.progress(0)
    for i, fdt in enumerate(fechas, 1):
        prog.progress(i / total)
        subtablas = []
        for nombre_csv, fuente in ARCHIVOS_DEMANDA.items():
            df = _procesar_csv_de_zip(fdt, "PDO", None, nombre_csv, fuente)
            if not df.empty:
                subtablas.append(df)
        for letra in RDO_LETRAS_GENERACION:
            for nombre_csv, fuente in ARCHIVOS_DEMANDA.items():
                df = _procesar_csv_de_zip(fdt, "RDO", letra, nombre_csv, fuente)
                if not df.empty:
                    subtablas.append(df)
        if subtablas:
            tablas.append(pd.concat(subtablas, ignore_index=True))
    prog.empty()
    return pd.concat(tablas, ignore_index=True) if tablas else pd.DataFrame()


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — MOTIVOS RDO
# ════════════════════════════════════════════════════════════════════════════════

def tab2_extraer_hora(df):
    try:
        val = df.iat[6, 1]
        return "" if pd.isna(val) else str(val).strip()
    except Exception:
        return ""


def tab2_extraer_motivo(df):
    import re
    if df is None or df.empty:
        return ""
    s = lambda x: "" if pd.isna(x) else str(x).strip()
    motivo = ""
    try:
        if df.shape[1] >= 3:
            colC = df.iloc[:, 2].astype(str).str.upper()
            idx  = colC[colC.str.contains("MOTIVO", na=False)].index
            if len(idx) > 0:
                fila = idx[0] + 1
                if fila < len(df):
                    for celda in df.iloc[fila, :].map(s).tolist():
                        if "-" in celda and re.search(r"-\s*[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", celda):
                            motivo = celda.strip()
                            break
    except Exception:
        pass
    if not motivo:
        try:
            if df.shape[1] >= 4:
                colD = [s(v) for v in df.iloc[:, 3].tolist()]
                colD = [v for v in colD if v]
                if colD:
                    motivo = colD[-1]
        except Exception:
            pass
    return motivo


def tab2_procesar_motivos_fecha(fecha) -> pd.DataFrame:
    fecha_dt = pd.Timestamp(fecha)
    d = f"{fecha_dt.day:02d}"
    m = f"{fecha_dt.month:02d}"
    registros = []
    for letra in "ABCDEFG":
        url  = _url_motivo(fecha_dt, letra)
        data = _get(url)
        if data is None or data[:4] != b"PK\x03\x04":
            continue
        try:
            df = pd.read_excel(io.BytesIO(data), header=None, engine="openpyxl")
            if df is None or df.empty:
                continue
            registros.append({
                "Fecha":   str(fecha),
                "Hora":    tab2_extraer_hora(df),
                "TipoRDO": f"RDO {letra}",
                "Motivo":  tab2_extraer_motivo(df),
                "Archivo": f"Reprog_{d}{m}{letra}.xlsx",
                "Ruta":    f"Motivos RDO/{fecha_dt.year}/{fecha}/Reprog_{d}{m}{letra}.xlsx",
            })
        except Exception:
            pass
    return pd.DataFrame(registros, columns=["Fecha","Hora","TipoRDO","Motivo","Archivo","Ruta"])


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — COSTO RDO
# ════════════════════════════════════════════════════════════════════════════════

TAB2_NOMBRE_COSTO = "Termica - Costo Operacion por Etapas ($).csv"

def _tab2_leer_costo(data):
    try:
        df = pd.read_csv(io.BytesIO(data))
        return df.apply(pd.to_numeric, errors="coerce")
    except Exception:
        return None

def _tab2_sumar_costo(df_num):
    if df_num is None or df_num.shape[1] <= 1:
        return 0.0
    return float(df_num.iloc[:, 1:].sum(axis=1, skipna=True).sum(skipna=True))

def _costo_del_zip(contenido):
    data = _buscar_en_zip(contenido, TAB2_NOMBRE_COSTO)
    if data is None:
        return None, 0, None
    df_num = _tab2_leer_costo(data)
    if df_num is None:
        return None, 0, None
    return _tab2_sumar_costo(df_num), len(df_num), df_num

def tab2_procesar_costos_fecha(fecha) -> pd.DataFrame:
    fecha_dt = pd.Timestamp(fecha)
    anio     = fecha_dt.year
    zip_pdo  = obtener_zip(fecha_dt, "PDO")
    subtotal_pdo, filas_pdo, df_pdo = _costo_del_zip(zip_pdo)
    if subtotal_pdo is None:
        return pd.DataFrame()
    resultados = [{
        "Fecha": str(fecha), "TipoSerie": "PDO", "RDO": "",
        "Subtotal": subtotal_pdo, "Filas": filas_pdo, "FilasPDO": filas_pdo,
        "DiferenciaFilas": 0, "CostoAdicionalPDO": 0.0, "CostoTotal": subtotal_pdo,
        "Archivo": TAB2_NOMBRE_COSTO,
        "Ruta": f"PDO/{anio}/{fecha}/{TAB2_NOMBRE_COSTO}",
    }]
    for letra in "ABCDEFG":
        zip_rdo = obtener_zip(fecha_dt, "RDO", letra)
        subtotal_rdo, filas_rdo, _ = _costo_del_zip(zip_rdo)
        if subtotal_rdo is None:
            continue
        diferencia      = filas_pdo - filas_rdo
        costo_adicional = _tab2_sumar_costo(df_pdo.head(diferencia)) if diferencia > 0 and df_pdo is not None else 0.0
        resultados.append({
            "Fecha": str(fecha), "TipoSerie": "RDO", "RDO": f"RDO {letra}",
            "Subtotal": subtotal_rdo, "Filas": filas_rdo, "FilasPDO": filas_pdo,
            "DiferenciaFilas": diferencia, "CostoAdicionalPDO": costo_adicional,
            "CostoTotal": subtotal_rdo + costo_adicional,
            "Archivo": TAB2_NOMBRE_COSTO,
            "Ruta": f"RDO/{anio}/{fecha}/{letra}/{TAB2_NOMBRE_COSTO}",
        })
    return pd.DataFrame(resultados)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 2 — INDICES ALFA/BETA/GAMMA
# ════════════════════════════════════════════════════════════════════════════════

def _tab2_encontrar_col(headers, palabra):
    palabra = palabra.lower()
    for i, h in enumerate(headers):
        if h == palabra: return i
    for i, h in enumerate(headers):
        if palabra in h:  return i
    return None

def tab2_procesar_indices_fecha(fecha) -> pd.DataFrame:
    fecha_dt = pd.Timestamp(fecha)
    d = f"{fecha_dt.day:02d}"; m = f"{fecha_dt.month:02d}"
    nombre = ruta = data = None
    for letra in "GFEDCBA":
        url = _url_indices(fecha_dt, letra)
        raw = _get(url)
        if raw and raw[:4] == b"PK\x03\x04":
            nombre = f"indices{fecha_dt.year}{m}{d}_{letra}.xlsx"
            ruta   = f"Indices/{fecha_dt.year}/{fecha}/{nombre}"
            data   = raw
            break
    if data is None:
        return pd.DataFrame()
    try:
        df = pd.read_excel(io.BytesIO(data), header=None, engine="openpyxl")
    except Exception:
        return pd.DataFrame()
    if df.empty:
        return pd.DataFrame()
    headers = ["" if pd.isna(x) else str(x).strip().lower() for x in df.iloc[0, :].tolist()]
    c_alfa  = _tab2_encontrar_col(headers, "alfa")
    c_beta  = _tab2_encontrar_col(headers, "beta")
    c_gamma = _tab2_encontrar_col(headers, "gamma")
    def col_vals(idx):
        if idx is None: return [np.nan] * 48
        vals = df.iloc[1:49, idx].tolist()
        out  = []
        for v in vals:
            if pd.isna(v): out.append(np.nan)
            else:
                try:    out.append(float(v))
                except: out.append(np.nan)
        while len(out) < 48: out.append(np.nan)
        return out[:48]
    alfa = col_vals(c_alfa); beta = col_vals(c_beta); gamma = col_vals(c_gamma)
    horas = []
    h, minuto = 0, 30
    for _ in range(48):
        horas.append(f"{h:02d}:{minuto:02d}")
        minuto += 30
        if minuto == 60: minuto = 0; h = (h + 1) % 24
    return pd.DataFrame([{
        "Fecha": str(fecha), "ArchivoIndices": nombre, "Periodo": i + 1,
        "Hora": horas[i], "ALFA": alfa[i], "BETA": beta[i], "GAMMA": gamma[i], "Ruta": ruta,
    } for i in range(48)])


def tab2_procesar_rango(fecha_inicio, fecha_fin, log):
    fechas = pd.date_range(pd.Timestamp(fecha_inicio), pd.Timestamp(fecha_fin), freq="D")
    total  = len(fechas)
    m_lst, c_lst, i_lst = [], [], []
    log.write(f"MOTIVOS / COSTOS / ÍNDICES — {fecha_inicio.date()} → {fecha_fin.date()} ({total} días)")
    prog = st.progress(0)
    for i, fdt in enumerate(fechas, 1):
        prog.progress(i / total)
        fecha = fdt.strftime("%Y-%m-%d")
        df_m = tab2_procesar_motivos_fecha(fecha)
        if not df_m.empty: m_lst.append(df_m)
        df_c = tab2_procesar_costos_fecha(fecha)
        if not df_c.empty: c_lst.append(df_c)
        df_i = tab2_procesar_indices_fecha(fecha)
        if not df_i.empty: i_lst.append(df_i)
    prog.empty()
    cat = lambda lst: pd.concat(lst, ignore_index=True) if lst else pd.DataFrame()
    return cat(m_lst), cat(c_lst), cat(i_lst)


# ════════════════════════════════════════════════════════════════════════════════
# TAB 3 — GENERACION RDO
# ════════════════════════════════════════════════════════════════════════════════

def _tab3_fila(df):
    if df is None or df.empty: return []
    if df.shape[1] > 1:
        return df.iloc[:, 1:].apply(pd.to_numeric, errors="coerce").sum(axis=1, skipna=True).tolist()
    return []

def _tab3_totales_rer(df, barras):
    if df is None or df.empty: return []
    columnas = {str(c).strip().upper(): c for c in df.columns}
    sel = [columnas[str(b).strip().upper()] for b in barras if str(b).strip().upper() in columnas]
    if not sel: return []
    return df[sel].apply(pd.to_numeric, errors="coerce").sum(axis=1, skipna=True).tolist()

def _tab3_pad48(vals):
    if not vals: return []
    vals = list(vals)
    if len(vals) < 48: vals = [0.0] * (48 - len(vals)) + vals
    return vals[:48]

def _tab3_suma(a, b):
    if not a or not b: return []
    n = min(len(a), len(b))
    return [(a[i] if pd.notna(a[i]) else 0.0) + (b[i] if pd.notna(b[i]) else 0.0) for i in range(n)]

def _tab3_agregar(salida, fecha, serie, recurso, valores):
    valores = _tab3_pad48(valores)
    if not valores: return
    for i, v in enumerate(valores):
        salida.append({"Fecha": str(fecha), "Serie": serie, "Periodo": i + 1,
                       "Hora": HORAS_GENERACION[i], "Recurso": recurso, "MW": v})

def _csv_zip(fecha_dt, tipo, letra, nombre):
    contenido = obtener_zip(fecha_dt, tipo, letra)
    data      = _buscar_en_zip(contenido, nombre)
    return _leer_csv(data) if data else None

def _tab3_hidro(fecha_dt, fecha, salida):
    dh = _csv_zip(fecha_dt, "PDO", None, ARCHIVO_HIDRO)
    dr = _csv_zip(fecha_dt, "PDO", None, ARCHIVO_RER)
    vh = _tab3_fila(dh); vr = _tab3_totales_rer(dr, BARRAS_HIDRO)
    if vh and vr:
        vals = _tab3_suma(_tab3_pad48(vh), _tab3_pad48(vr))
        if vals: _tab3_agregar(salida, fecha, "PDO", "HIDRO", vals)
    for letra in RDO_LETRAS_GENERACION:
        dh = _csv_zip(fecha_dt, "RDO", letra, ARCHIVO_HIDRO)
        dr = _csv_zip(fecha_dt, "RDO", letra, ARCHIVO_RER)
        vh = _tab3_fila(dh); vr = _tab3_totales_rer(dr, BARRAS_HIDRO)
        if not vh or not vr: continue
        vals = _tab3_suma(_tab3_pad48(vh), _tab3_pad48(vr))
        if vals: _tab3_agregar(salida, fecha, f"RDO {letra}", "HIDRO", vals)

def _tab3_eolica(fecha_dt, fecha, salida):
    barras = [x.upper() for x in BARRAS_EOLICA]
    df = _csv_zip(fecha_dt, "PDO", None, ARCHIVO_RER)
    vals = _tab3_totales_rer(df, barras)
    if vals: _tab3_agregar(salida, fecha, "PDO", "EOLICA", _tab3_pad48(vals))
    for letra in RDO_LETRAS_GENERACION:
        df   = _csv_zip(fecha_dt, "RDO", letra, ARCHIVO_RER)
        vals = _tab3_totales_rer(df, barras)
        if vals: _tab3_agregar(salida, fecha, f"RDO {letra}", "EOLICA", _tab3_pad48(vals))

def _tab3_solar(fecha_dt, fecha, salida):
    barras = [x.upper() for x in BARRAS_SOLAR]
    df = _csv_zip(fecha_dt, "PDO", None, ARCHIVO_RER)
    vals = _tab3_totales_rer(df, barras)
    if vals: _tab3_agregar(salida, fecha, "PDO", "SOLAR", _tab3_pad48(vals))
    for letra in RDO_LETRAS_GENERACION:
        df   = _csv_zip(fecha_dt, "RDO", letra, ARCHIVO_RER)
        vals = _tab3_totales_rer(df, barras)
        if not vals: continue
        if not any(pd.notna(v) and float(v) != 0 for v in vals): continue
        _tab3_agregar(salida, fecha, f"RDO {letra}", "SOLAR", _tab3_pad48(vals))

def _tab3_termica(fecha_dt, fecha, salida):
    barras = [x.upper() for x in BARRAS_TERMICA_RER]
    dt = _csv_zip(fecha_dt, "PDO", None, ARCHIVO_TERMICA)
    dr = _csv_zip(fecha_dt, "PDO", None, ARCHIVO_RER)
    vt = _tab3_fila(dt); vr = _tab3_totales_rer(dr, barras)
    if vt and vr:
        vals = _tab3_suma(_tab3_pad48(vt), _tab3_pad48(vr))
        if vals: _tab3_agregar(salida, fecha, "PDO", "TERMICA", vals)
    for letra in RDO_LETRAS_GENERACION:
        dt = _csv_zip(fecha_dt, "RDO", letra, ARCHIVO_TERMICA)
        dr = _csv_zip(fecha_dt, "RDO", letra, ARCHIVO_RER)
        vt = _tab3_fila(dt); vr = _tab3_totales_rer(dr, barras)
        if not vt or not vr: continue
        vals = _tab3_suma(_tab3_pad48(vt), _tab3_pad48(vr))
        if vals: _tab3_agregar(salida, fecha, f"RDO {letra}", "TERMICA", vals)

def tab3_procesar_fecha(fecha_dt):
    fecha = fecha_dt.date()
    filas = []
    for nombre, fn in [("HIDRO", _tab3_hidro), ("EOLICA", _tab3_eolica),
                        ("SOLAR", _tab3_solar), ("TERMICA", _tab3_termica)]:
        try:
            fn(fecha_dt, fecha, filas)
        except Exception:
            pass
    return pd.DataFrame(filas, columns=["Fecha","Serie","Periodo","Hora","Recurso","MW"])

def tab3_procesar_rango(fecha_inicio, fecha_fin, log):
    fechas = pd.date_range(fecha_inicio, fecha_fin, freq="D")
    log.write(f"GENERACION RDO — {fecha_inicio.date()} → {fecha_fin.date()} ({len(fechas)} días)")
    prog   = st.progress(0)
    tablas = []
    for i, fdt in enumerate(fechas, 1):
        prog.progress(i / len(fechas))
        df_f = tab3_procesar_fecha(fdt)
        if not df_f.empty:
            tablas.append(df_f)
    prog.empty()
    return pd.concat(tablas, ignore_index=True) if tablas else pd.DataFrame()


# ════════════════════════════════════════════════════════════════════════════════
# STREAMLIT UI
# ════════════════════════════════════════════════════════════════════════════════

st.set_page_config(page_title="Actualización de Datos COES", page_icon=None, layout="centered")

# ── Estilos modernos ──────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Fondo general — azul oscuro Osinergmin */
[data-testid="stAppViewContainer"] {
    background: linear-gradient(160deg, #001a5e, #0039AA, #001233);
    min-height: 100vh;
    overflow: hidden;
}
[data-testid="stHeader"] { background: transparent; }

/* Esferas animadas — movimiento vertical */
[data-testid="stAppViewContainer"]::before,
[data-testid="stAppViewContainer"]::after {
    content: "";
    position: fixed;
    border-radius: 50%;
    filter: blur(65px);
    opacity: 0.45;
    pointer-events: none;
    z-index: 0;
}
[data-testid="stAppViewContainer"]::before {
    width: 650px; height: 650px;
    background: radial-gradient(circle, #FCE122, #E08A00);
    top: -200px; left: -150px;
    animation: floatUp 9s ease-in-out infinite alternate;
}
[data-testid="stAppViewContainer"]::after {
    width: 550px; height: 550px;
    background: radial-gradient(circle, #0039AA, #12996B);
    bottom: -150px; right: -120px;
    animation: floatDown 11s ease-in-out infinite alternate;
}

@keyframes floatUp {
    0%   { transform: translateY(0px) scale(1); }
    50%  { transform: translateY(80px) scale(1.06); }
    100% { transform: translateY(160px) scale(0.96); }
}
@keyframes floatDown {
    0%   { transform: translateY(0px) scale(1); }
    50%  { transform: translateY(-80px) scale(1.05); }
    100% { transform: translateY(-160px) scale(0.97); }
}

/* Puntitos decorativos */
[data-testid="stMain"] {
    background-image: radial-gradient(rgba(255,255,255,0.05) 1px, transparent 1px);
    background-size: 28px 28px;
    position: relative;
    z-index: 1;
}

/* Título */
.titulo-app {
    text-align: center;
    font-size: 2.4rem;
    font-weight: 800;
    color: #ffffff;
    margin-bottom: 0.2rem;
    letter-spacing: -0.5px;
}
.subtitulo-app {
    text-align: center;
    color: #a0a8c0;
    font-size: 0.95rem;
    margin-bottom: 2rem;
}

/* Tarjetas de estado */
.card-estado {
    background: rgba(255,255,255,0.06);
    border: 1px solid rgba(255,255,255,0.10);
    border-radius: 14px;
    padding: 1rem 1.3rem;
    margin-bottom: 0.6rem;
    display: flex;
    justify-content: space-between;
    align-items: center;
    backdrop-filter: blur(8px);
}
.card-tabla  { font-weight: 600; color: #e0e6ff; font-size: 0.95rem; }
.card-fecha  { color: #8892b0; font-size: 0.85rem; }
.badge-ok    { background: #0a3322; color: #12996B; border-radius: 20px;
               padding: 3px 14px; font-size: 0.8rem; font-weight: 700;
               border: 1px solid #12996B44; }
.badge-warn  { background: #3a2800; color: #FCE122; border-radius: 20px;
               padding: 3px 14px; font-size: 0.8rem; font-weight: 700;
               border: 1px solid #FCE12244; }
.badge-err   { background: #3a0a0a; color: #D14343; border-radius: 20px;
               padding: 3px 14px; font-size: 0.8rem; font-weight: 700;
               border: 1px solid #D1434344; }

/* Label del date_input */
label { color: #a0a8c0 !important; font-size: 0.85rem !important; }

/* Sección subtítulo */
.seccion-titulo {
    color: #c9d1f5;
    font-size: 1.05rem;
    font-weight: 700;
    margin: 1.5rem 0 0.8rem 0;
    text-transform: uppercase;
    letter-spacing: 1px;
}
</style>
""", unsafe_allow_html=True)

# ── Encabezado ────────────────────────────────────────────────────────────────
st.markdown('<div class="titulo-app">Actualización de Datos COES</div>', unsafe_allow_html=True)
st.markdown('<div class="subtitulo-app">Consulta el estado de los datos y actualiza la información pendiente del COES.</div>', unsafe_allow_html=True)

# ── Fecha fin seleccionable (por defecto: hoy) ───────────────────────────────
hoy = pd.Timestamp.today().normalize()
fecha_fin_sel = st.date_input(
    "Fecha de actualización",
    value=hoy.date(),
    min_value=FECHA_INICIO_INICIAL.date(),
    max_value=hoy.date(),
)
FECHA_FIN = pd.Timestamp(fecha_fin_sel)

# ── Estado actual de la base ──────────────────────────────────────────────────
st.markdown('<div class="seccion-titulo">ESTADO DE LOS DATOS</div>', unsafe_allow_html=True)

TABLAS = ["DEMANDA", MOTIVOS_SHEET, COSTOS_SHEET, INDICES_SHEET, GENERACION_SHEET]

@st.cache_data(ttl=60, show_spinner="Consultando fechas en Base de Datos …")
def _cargar_max_fechas():
    resultado = {}
    for t in TABLAS:
        mx = _max_fecha_pg(t)
        resultado[t] = mx
    return resultado

max_fechas = _cargar_max_fechas()

todo_ok = True
for tabla in TABLAS:
    mx = max_fechas.get(tabla)
    fecha_str = str(mx.date()) if mx else "sin datos"
    dias_atraso = (FECHA_FIN - mx).days if mx else None
    if mx and mx >= FECHA_FIN:
        badge = '<span class="badge-ok">✓ Al día</span>'
    elif mx:
        badge = f'<span class="badge-warn">Pendiente: {dias_atraso} día(s)</span>'
        todo_ok = False
    else:
        badge = '<span class="badge-err">✕ Sin datos</span>'
        todo_ok = False

    st.markdown(f"""
    <div class="card-estado">
        <div>
            <div class="card-tabla">{tabla}</div>
            <div class="card-fecha">Último registro: {fecha_str}</div>
        </div>
        {badge}
    </div>
    """, unsafe_allow_html=True)

st.markdown("<br>", unsafe_allow_html=True)

if todo_ok:
    st.markdown(f"""
    <div style="background: rgba(18,153,107,0.15); border: 1px solid #12996B;
                border-radius: 12px; padding: 0.9rem 1.2rem; margin-top: 0.5rem;">
        <span style="color: #12996B; font-weight: 600; font-size: 0.95rem;">
            ✅ Todas las tablas están al día hasta <b>{FECHA_FIN.date()}</b>.
        </span>
    </div>
    """, unsafe_allow_html=True)
else:
    st.markdown("""
    <div style="background: rgba(252,225,34,0.12); border: 1px solid #FCE122;
                border-radius: 12px; padding: 0.9rem 1.2rem; margin-top: 0.5rem;">
        <span style="color: #FCE122; font-weight: 600; font-size: 0.95rem;">
            ⚠️ Hay información pendiente de actualización. Presiona <b>Actualizar datos</b> para completar.
        </span>
    </div>
    """, unsafe_allow_html=True)

# ── Botón de actualización ────────────────────────────────────────────────────
st.divider()
if st.button("Actualizar datos", type="primary", width="stretch"):
    log_box = st.container()
    def log_msg(msg):
        log_box.markdown(f"""
        <div style="background: rgba(255,255,255,0.07); border-left: 3px solid #0039AA;
                    border-radius: 6px; padding: 0.5rem 1rem; margin-bottom: 0.4rem;
                    color: #e0e6ff; font-size: 0.88rem; font-family: monospace;">
            {msg}
        </div>""", unsafe_allow_html=True)

    class _Log:
        def write(self, msg): log_msg(msg)
    log = _Log()
    resumen = {}

    with st.spinner("Conectando a Supabase…"):
        try:
            con = _pg_con()
        except Exception as e:
            st.error(f"No se pudo conectar a Supabase: {e}")
            st.stop()

    # ── DEMANDA ───────────────────────────────────────────────────────────────
    mx_dem = max_fechas.get("DEMANDA")
    inicio_dem = FECHA_INICIO_INICIAL if mx_dem is None else mx_dem   # incluye el último día guardado
    if inicio_dem <= FECHA_FIN:
        df_dem = procesar_demanda_rango(inicio_dem, FECHA_FIN, log)
        n = _insertar_df_pg(df_dem, "DEMANDA", con)
        resumen["DEMANDA"] = n
        log.write(f"  → {n:,} filas insertadas en DEMANDA")
    else:
        log.write("DEMANDA ya está al día ✅")
        resumen["DEMANDA"] = 0

    # ── MOTIVOS / COSTOS / ÍNDICES ────────────────────────────────────────────
    mx_m = max_fechas.get(MOTIVOS_SHEET)
    mx_c = max_fechas.get(COSTOS_SHEET)
    mx_i = max_fechas.get(INDICES_SHEET)
    inicio_m = FECHA_INICIO_INICIAL if mx_m is None else mx_m   # incluye el último día
    inicio_c = FECHA_INICIO_INICIAL if mx_c is None else mx_c
    inicio_i = FECHA_INICIO_INICIAL if mx_i is None else mx_i
    inicio_mci = min(inicio_m, inicio_c, inicio_i)

    if inicio_mci <= FECHA_FIN:
        df_m, df_c, df_i = tab2_procesar_rango(inicio_mci, FECHA_FIN, log)
        nm = _insertar_df_pg(df_m, MOTIVOS_SHEET,  con)
        nc = _insertar_df_pg(df_c, COSTOS_SHEET,   con)
        ni = _insertar_df_pg(df_i, INDICES_SHEET,  con)
        resumen[MOTIVOS_SHEET] = nm
        resumen[COSTOS_SHEET]  = nc
        resumen[INDICES_SHEET] = ni
        log.write(f"  → Motivos: {nm:,} | Costos: {nc:,} | Índices: {ni:,} filas insertadas")
    else:
        log.write("Motivos/Costos/Índices ya están al día ✅")
        resumen[MOTIVOS_SHEET] = resumen[COSTOS_SHEET] = resumen[INDICES_SHEET] = 0

    # ── GENERACION ────────────────────────────────────────────────────────────
    mx_gen = max_fechas.get(GENERACION_SHEET)
    inicio_gen = FECHA_INICIO_INICIAL if mx_gen is None else mx_gen   # incluye el último día
    if inicio_gen <= FECHA_FIN:
        df_gen = tab3_procesar_rango(inicio_gen, FECHA_FIN, log)
        ng = _insertar_df_pg(df_gen, GENERACION_SHEET, con)
        resumen[GENERACION_SHEET] = ng
        log.write(f"  → {ng:,} filas insertadas en GENERACION_RDO")
    else:
        log.write("GENERACION_RDO ya está al día ✅")
        resumen[GENERACION_SHEET] = 0

    con.close()

    # ── Resumen final ─────────────────────────────────────────────────────────
    total_insertadas = sum(resumen.values())
    st.success(f"✅ Actualización completada — {total_insertadas:,} filas nuevas en total.")
    st.cache_data.clear()   # fuerza recarga de fechas al próximo refresh
    st.rerun()