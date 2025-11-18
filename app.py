from __future__ import annotations
from pathlib import Path
from datetime import datetime, date, timedelta
import io, zipfile, math, requests
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.backends.backend_pdf import PdfPages
import streamlit as st
from math import isfinite, isnan
import numpy as np
import re, unicodedata
from zoneinfo import ZoneInfo
import plotly.graph_objects as go

import warnings
warnings.filterwarnings(
    "ignore",
    message="Attempting to set identical low and high ylims makes transformation singular"
)

# ------------------ Configuración ------------------
BARRAS_DEF = ["SANTA ROSA 220 A", "MOQUEGUA 220", "ZORRITOS 220"]
RDO_LETRAS_DEF = list("ABCDEF")
MES_TXT = ["ENERO","FEBRERO","MARZO","ABRIL","MAYO","JUNIO","JULIO","AGOSTO","SETIEMBRE","OCTUBRE","NOVIEMBRE","DICIEMBRE"]

base_pdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FPrograma%20Diario%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FYUPANA_{y}{m}{d}.zip")
base_rdo = ("https://www.coes.org.pe/portal/browser/download?"
            "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
            "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{d}{m}{letra}%2FYUPANA_{d}{m}{letra}.zip")
base_ieod = ("https://www.coes.org.pe/portal/browser/download?"
             "url=Post%20Operaci%C3%B3n%2FReportes%2FIEOD%2F"
             "{y}%2F{m}_{M}%2F{d}%2FAnexoA_{ddmm}.xlsx")
base_motivo = ("https://www.coes.org.pe/portal/browser/download?"
               "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
               "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{dd}{mm}{L}%2FReprog_{dd}{mm}{L}.xlsx")
base_indices = ("https://www.coes.org.pe/portal/browser/download?"
                "url=Operaci%C3%B3n%2FPrograma%20de%20Operaci%C3%B3n%2FReprograma%20Diario%20Operaci%C3%B3n%2F"
                "{y}%2F{m}_{M}%2FD%C3%ADa%20{d}%2FReprog%20{dd}{mm}{L}%2Findices{y}{mm}{dd}_{L}.xlsx")

# ------------------ Utilidades ------------------
def _descargar_y_extraer_zip(url: str, destino: Path) -> bool:
    try:
        r = requests.get(url, timeout=40); r.raise_for_status()
        if not r.content.startswith(b"PK\x03\x04"): return False
        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
            if zf.testzip(): return False
            destino.mkdir(parents=True, exist_ok=True)
            zf.extractall(destino)
        return True
    except Exception:
        return False

def cargar_dataframe(folder: Path, stem: str) -> pd.DataFrame | None:
    for ext in (".csv", ".CSV", ".xlsx", ".xls"):
        f = folder / f"{stem}{ext}"
        if f.exists():
            try:
                if f.suffix.lower() in (".xlsx", ".xls"):
                    return pd.read_excel(f, engine="openpyxl")
                return pd.read_csv(f, sep=",", engine="python")
            except Exception:
                return None
    return None

def extraer_columna(df: pd.DataFrame, col: str):
    return df[col].tolist() if df is not None and col in df.columns else None

def rellenar_hasta_48(lst):
    if not lst: return None
    faltan = 48 - len(lst)
    return ([0]*faltan + lst) if faltan > 0 else lst[:48]

def recortar_ceros_inicio(vals, hrs):
    for i, v in enumerate(vals):
        if v != 0:
            return hrs[i:], vals[i:]
    return [], []

def fila_sin_primer_valor(df: pd.DataFrame) -> list | None:
    if df is None or df.empty: return None
    if df.shape[1] > 1:
        return df.iloc[:,1:].sum(axis=1, numeric_only=True).tolist()
    tot=[]
    for celda in df.iloc[:,0].astype(str):
        nums=[float(x) for x in celda.split(",")[1:] if x.strip()]
        tot.append(sum(nums))
    return tot

def suma_elementos(*listas):
    out = [0]*48
    for lst in listas:
        if lst:
            for i, v in enumerate(lst[:48]):
                out[i] += v
    return out

def _sin_acentos(s: str) -> str:
    return unicodedata.normalize("NFKD", str(s)).encode("ASCII", "ignore").decode()

def _lee_ieod_bytes(y2, m2, M2, d2):
    ddmm2 = f"{d2:02d}{m2:02d}"
    url  = base_ieod.format(y=y2, m=f"{m2:02d}", M=M2, d=f"{d2:02d}", ddmm=ddmm2)
    r = requests.get(url, timeout=40); r.raise_for_status()
    return io.BytesIO(r.content)

def _extrae_demanda_48(fbytes):
    df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
    col_demanda = None
    for c in df.columns:
        if isinstance(c, str):
            c_norm = _sin_acentos(c).upper().strip()
            if "TOTAL" in c_norm:
                col_demanda = c; break
    if not col_demanda: return None
    vals = pd.to_numeric(df[col_demanda].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
    return (vals + [0.0]*48)[:48]

# ---- Motivos RDO ----
def _leer_excel_motivo(path: Path):
    try: return pd.read_excel(path, header=None, engine="openpyxl")
    except Exception: return None

def _extraer_motivo(df: pd.DataFrame) -> str:
    import re
    if df is None or df.empty: return ""
    s = lambda x: "" if pd.isna(x) else str(x).strip()
    motivo = ""
    try:
        colC = df.iloc[:, 2].astype(str).str.upper()
        idx = colC[colC.str.contains("MOTIVO", na=False)].index
        if len(idx) > 0:
            r = idx[0] + 1
            if r < len(df):
                for cel in df.iloc[r, :].map(s).tolist():
                    if "-" in cel and re.search(r"-\s*[A-Za-zÁÉÍÓÚÜÑáéíóúüñ]", cel):
                        motivo = cel.strip(); break
    except Exception: pass
    if not motivo:
        try:
            colD = [s(v) for v in df.iloc[:, 3].tolist()]
            colD = [v for v in colD if v]
            if colD: motivo = colD[-1]
        except Exception: pass
    return motivo

def _extraer_hora(df: pd.DataFrame) -> str:
    try:
        val = df.iat[6, 1]
        return "" if pd.isna(val) else str(val).strip()
    except Exception:
        return ""

def recolectar_motivos_dia(y, m, d, M, destino, letras="ABCDEF"):
    datos = []
    for L in letras:
        url = base_motivo.format(y=y, m=m, M=M, d=d, dd=d, mm=m, L=L)
        out = destino / f"Reprog_{y}{m}{d}_{L}.xlsx"
        if not out.exists():
            try:
                r = requests.get(url, timeout=40)
                if not (r.status_code == 200 and r.content.startswith(b"PK")): continue
                out.write_bytes(r.content)
            except Exception:
                continue
        df = _leer_excel_motivo(out)
        if df is None or df.empty: continue
        datos.append({"FECHA": f"{y}-{m}-{d}",
                      "HORA": _extraer_hora(df),
                      "TIPO DE RDO": L,
                      "MOTIVO": _extraer_motivo(df)})
    return pd.DataFrame(datos, columns=["FECHA","HORA","TIPO DE RDO","MOTIVO"])

# ---- Costo total ----
def leer_termica_csv(csv_path: Path):
    # lee CSV y convierte todo a numérico
    df = pd.read_csv(csv_path)
    df_num = df.apply(pd.to_numeric, errors="coerce")
    return df_num

def sumar_costo_termica_dataframe(df_num: pd.DataFrame):
    # suma desde la segunda columna en adelante por fila, luego suma todas las filas
    if df_num.shape[1] > 1:
        suma_por_fila = df_num.iloc[:, 1:].sum(axis=1, skipna=True)
    else:
        suma_por_fila = pd.Series(dtype=float)
    subtotal = suma_por_fila.sum(skipna=True)
    return float(subtotal), suma_por_fila

def procesar_raiz(raiz_dir: Path):
    if not raiz_dir.exists():
        return 0.0, 0, None, None

    candidatos_yupana = list(raiz_dir.glob("YUPANA_*"))
    if not candidatos_yupana:
        return 0.0, 0, None, None

    subtotal_total = 0.0
    filas_total = 0
    archivo_usado = None
    dfs_acumulados = []

    for yupa_dir in candidatos_yupana:
        csv_path = (
            yupa_dir
            / "RESULTADOS"
            / "Otros"
            / "Termica - Costo Operacion por Etapas ($).csv"
        )
        if not csv_path.exists():
            continue

        df_num = leer_termica_csv(csv_path)
        subtotal_archivo, _ = sumar_costo_termica_dataframe(df_num)

        subtotal_total += subtotal_archivo
        filas_total += len(df_num)
        archivo_usado = csv_path
        dfs_acumulados.append(df_num)

    if dfs_acumulados:
        df_concat = pd.concat(dfs_acumulados, ignore_index=True)
    else:
        df_concat = None

    return subtotal_total, filas_total, df_concat, archivo_usado

# ---- Índices (Alfa/Beta/Gamma) ----
def _descargar_y_validar_indices(y, m, d, M, destino, L):
    out = destino / f"indices_{y}{m}{d}_{L}.xlsx"
    if not out.exists():
        url = base_indices.format(y=y, m=m, M=M, d=d, dd=d, mm=m, L=L)
        try:
            r = requests.get(url, timeout=40)
            if not (r.status_code == 200 and r.content.startswith(b"PK")): return None
            out.write_bytes(r.content)
        except Exception:
            return None
    return out

def _headers_lower(df):
    if df is None or df.empty: return []
    row0 = df.iloc[0, :].tolist()
    return [("" if pd.isna(x) else str(x).strip().lower()) for x in row0]

def _find_col(headers, key):
    try: return headers.index(key)
    except ValueError: pass
    for i, h in enumerate(headers):
        if key in h: return i
    return None

def _pad_or_trim_48(vals):
    cleaned = []
    for v in (vals or []):
        if v is None: cleaned.append(float('nan'))
        else:
            try: cleaned.append(float(v))
            except Exception: cleaned.append(float('nan'))
    if len(cleaned) < 48: cleaned += [float('nan')] * (48 - len(cleaned))
    elif len(cleaned) > 48: cleaned = cleaned[:48]
    return cleaned

def _build_halfhour_labels():
    labels = []; h, mm = 0, 30
    for _ in range(48):
        labels.append(f"{h:02d}:{mm:02d}")
        mm += 30
        if mm == 60: mm = 0; h = (h + 1) % 24
    return labels

def extraer_listas_alfa_beta_gamma_ultimo(y, m, d, M, destino):
    for L in "FEDCB":
        p = _descargar_y_validar_indices(y, m, d, M, destino, L)
        if p is None: continue
        df = pd.read_excel(p, header=None, engine="openpyxl")
        if df is None or df.empty: continue
        headers = _headers_lower(df)
        c_alfa  = _find_col(headers, "alfa")
        c_beta  = _find_col(headers, "beta")
        c_gamma = _find_col(headers, "gamma")
        def col_vals(idx):
            if idx is None: return []
            vals = df.iloc[1:49, idx].tolist()
            return [None if pd.isna(v) else v for v in vals]
        return {"reprograma": L,
                "alfa":  col_vals(c_alfa),
                "beta":  col_vals(c_beta),
                "gamma": col_vals(c_gamma)}
    return {"reprograma": None, "alfa": [], "beta": [], "gamma": []}

def _plot_series(xlbls, yvals, titulo):
    fig, ax = plt.subplots(figsize=(11, 5))
    x = np.arange(len(xlbls))
    ax.plot(x, yvals, marker="o", linewidth=2, label=titulo)

    # === X igual al formato de aplicar_formato_xy ===
    L = len(xlbls)
    try:
        tickpos_base = ticks_pos  # si ya lo tienes global, úsalo
    except NameError:
        tickpos_base = list(range(0, L, 2))  # fallback cada 1h

    tickpos = [i for i in tickpos_base if i < L]
    ticklbl = [xlbls[i] for i in tickpos]
    ax.set_xticks(tickpos)
    ax.set_xticklabels(ticklbl, rotation=90, fontsize=10, ha="center")
    ax.set_xlim(-0.5, max(0, L-1) + 0.5)  # margen lateral (xpad=0.5)

    # === “Aire” en Y como aplicar_formato_xy ===
    y = np.array(yvals, dtype=float)
    y = y[np.isfinite(y)]
    if y.size:
        ymin, ymax = float(np.min(y)), float(np.max(y))
        if np.isfinite(ymin) and np.isfinite(ymax):
            pad = (0.05 * (abs(ymin) if ymin == ymax else (ymax - ymin)))
            ax.set_ylim(ymin - pad, ymax + pad)

    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_title(titulo); ax.set_ylabel("Valor")
    plt.tight_layout()
    return fig

def _plot_series_pdf(xs_labels, ys, title, pdf):
    fig = _plot_series(xs_labels, ys, title)
    pdf.savefig(fig); plt.close(fig)

# ---- CMG helpers ----
def _build_time_labels_and_ticks():
    inicio = datetime(2000, 1, 1, 0, 30)
    horas  = [(inicio + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
    horas[-1] = "23:59"
    ticks_pos = list(range(0, 48, 2))
    ticks_lbl = [horas[i] for i in ticks_pos]
    return horas, ticks_pos, ticks_lbl

def _plot_cmg_barra_en_axes(ax, barra, series_barra, horas, ticks_pos, ticks_lbl):
    valores_plot = []
    for nombre, valores in series_barra.items():
        x, y = recortar_ceros_inicio(valores, horas)
        if not y: continue
        valores_plot.extend(y)
        ax.plot(x, y, marker="o", linewidth=2, label=nombre)
    if not valores_plot: return False
    min_y = max(0, math.floor(min(valores_plot)) - 2)
    max_y = math.ceil(max(valores_plot)) + 2
    ax.set_ylim(min_y, max_y)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=10)
    ax.set_title(f"CMG {barra}")
    ax.set_ylabel("USD/MWh")
    ax.legend()
    return True

def asegurar_insumos_para_cmg(y: int, m: str, d: str, M: str, work_dir: Path, rdo_letras: list[str]):
    fecha_str = f"{y}{m}{d}"
    url_pdo = base_pdo.format(y=y, m=m, M=M, d=d)
    dir_pdo = work_dir / f"PDO_{fecha_str}"
    if not dir_pdo.exists() or not any(dir_pdo.iterdir()):
        _descargar_y_extraer_zip(url_pdo, dir_pdo)
    for L in rdo_letras:
        url_rdo = base_rdo.format(y=y, m=m, M=M, d=d, letra=L)
        dir_rdo = work_dir / f"RDO_{L}_{fecha_str}"
        if not dir_rdo.exists() or not any(dir_rdo.iterdir()):
            _descargar_y_extraer_zip(url_rdo, dir_rdo)

# ---- HIDRO helpers ----
def totales_hidro(df):
    if df is None or df.empty: return None
    if df.shape[1] > 1:
        return df.iloc[:,1:].sum(axis=1, numeric_only=True).tolist()
    tot=[]; 
    for celda in df.iloc[:,0].astype(str):
        nums=[float(x) for x in celda.split(",")[1:] if x.strip()]
        tot.append(sum(nums))
    return tot

def totales_rer(df, nombres):
    if df is None or df.empty: return None
    if df.shape[1] > 1:
        cols=[c for c in df.columns if str(c).strip().upper() in nombres]
        return df[cols].sum(axis=1, numeric_only=True).tolist() if cols else None
    enc=[h.strip().upper() for h in str(df.iloc[0,0]).split(",")]
    idx=[i for i,h in enumerate(enc) if h in nombres]
    if not idx: return None
    tot=[]
    for fila in df.iloc[1:,0].astype(str):
        partes=[p.strip() for p in fila.split(",")]
        nums=[float(partes[i]) if i<len(partes) and partes[i] else 0 for i in idx]
        tot.append(sum(nums))
    return tot

def aplicar_formato_xy(ax, L, ticks_pos, horas, y_values=None, ypad=0.05, xpad=0.5):
    # X: mismos ticks/etiquetas que la 1.ª, filtrados por longitud L
    tickpos = [i for i in ticks_pos if i < L]
    ticklbl = [horas[i] for i in tickpos]
    ax.set_xticks(tickpos)
    ax.set_xticklabels(ticklbl, rotation=90, fontsize=10, ha="center")
    # Margen lateral (inicio/fin) para que no esté pegado al borde
    ax.set_xlim(-xpad, max(0, L - 1) + xpad)

    # Y: “aire” para que las curvas no choquen con el borde
    if y_values is not None and len(y_values) > 0:
        y = np.array(y_values, dtype=float)
        y = y[np.isfinite(y)]
        if y.size:
            ymin, ymax = float(np.min(y)), float(np.max(y))
            if np.isfinite(ymin) and np.isfinite(ymax):
                if ymin == ymax:
                    pad = 0.05 * (abs(ymin) if ymin != 0 else 1.0)
                else:
                    pad = ypad * (ymax - ymin)
                ax.set_ylim(ymin - pad, ymax + pad)

def _nz(x):
    try:
        if x is None:
            return 0.0
        v = float(x)
        if not isfinite(v) or isnan(v):
            return 0.0
        return v
    except Exception:
        return 0.0

def _rel_err_abs_pct(den, num):
    d_ = _nz(den); n_ = _nz(num)
    if d_ == 0.0:
        return 0.0
    return abs((n_ - d_) / d_) * 100.0

def _omit_0_100(v, tol=1e-9):
    try:
        f = float(v)
        if not isfinite(f):
            return np.nan
        if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol:
            return np.nan
        return f
    except Exception:
        return np.nan

def _norm(txt: str) -> str:
    s = unicodedata.normalize("NFKD", str(txt)).encode("ASCII","ignore").decode("ASCII")
    return re.sub(r"\s+", " ", s.strip().upper())

def _find_cols_ieod(df):
    c_pas = c_reg = None
    for c in df.columns:
        k = _norm(c)
        if k == "H. PASADA" and c_pas is None: c_pas = c
        if k == "H. REGULACION" and c_reg is None: c_reg = c
    return c_pas, c_reg

def _plot_indices_pdf(x_labels, y_vals, title, pdf, marcas_x=None, marcas_lbl=None):
    fig, ax = plt.subplots(figsize=(11, 5))
    xs = list(range(48))
    ax.plot(xs, y_vals, linewidth=2, marker='o')

    ax.set_xticks(range(48))
    ax.set_xticklabels([x_labels[i] for i in range(48)], rotation=90)
    for lbl in ax.get_xticklabels():
        lbl.set_ha('center')

    plt.subplots_adjust(bottom=0.25)
    ax.set_xlim(0, 47)
    ax.grid(True, which="both", alpha=0.3)
    ax.set_title(title)
    ax.set_ylabel("Valor")
    ax.set_ylim(bottom=0)

    # Líneas rojas punteadas + etiquetas
    if marcas_x:
        for i, x_pos in enumerate(marcas_x):
            if 0 <= x_pos < 48:
                ax.axvline(x=x_pos, color='red', linestyle='--', linewidth=1)
                lbl_txt = (marcas_lbl[i] if (marcas_lbl and i < len(marcas_lbl)) else None)
                if lbl_txt:
                    y_top = ax.get_ylim()[1]
                    ax.text(x_pos + 0.3, y_top * 0.9, lbl_txt, color='red',
                            fontsize=8, rotation=90, va='top', ha='left')

    plt.tight_layout()
    pdf.savefig(fig)
    plt.close(fig)

# -----------------------------------------------------------------------------
# ------------------------------- PANTALLA ------------------------------------
# -----------------------------------------------------------------------------
def render_graficos_en_pantalla(ini: date, fin: date, barras: list[str], rdo_letras: list[str], work_dir: Path):
    # ===== Helpers de tiempo =====
    def _ticks_30m():
        inicio = datetime(2000, 1, 1, 0, 30)
        horas  = [(inicio + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
        horas[-1] = "23:59"
        ticks_pos = list(range(0, 48, 2))
        ticks_lbl = [horas[i] for i in ticks_pos]
        return horas, ticks_pos, ticks_lbl

    horas, ticks_pos, ticks_lbl = _ticks_30m()

    # ===== Variables de fecha/paths =====
    y, m, d = fin.year, f"{fin.month:02d}", f"{fin.day:02d}"
    ddmm = f"{d}{m}"
    M = MES_TXT[int(m) - 1]
    fecha_str = f"{y}{m}{d}"
    pdo_res = work_dir / f"PDO_{fecha_str}" / f"YUPANA_{fecha_str}" / "RESULTADOS"
    
    # ==== Pestañas ====
    tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs(["Demanda", "Motivos, Costo Total e Índices", "Hidro, Eólico y Solar", "CMG" , "Histórico del IEOD","Histórico de Potencia y Energía", "Térmicas"])
    
    with tab1:
        # =========================================================
        # ==================== DEMANDA Y ERROR ====================
        # =========================================================
        st.markdown("### DEMANDA")
        demanda_figs1 = []
        try:
            archivos_dem = {
                "HIDRO"   : "Hidro - Despacho (MW)",
                "TERMICA" : "Termica - Despacho (MW)",
                "RER"     : "Rer y No COES - Despacho (MW)"
            }
        
            # === DEMANDA (PDO + RDOs) ===
            series_dem = {}
            vals_hidro_p   = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos_dem["HIDRO"])))
            vals_termica_p = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos_dem["TERMICA"])))
            vals_rer_p     = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos_dem["RER"])))
            series_dem["PDO"] = suma_elementos(vals_hidro_p, vals_termica_p, vals_rer_p)
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos_dem["HIDRO"])))
                vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos_dem["TERMICA"])))
                vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos_dem["RER"])))
                if any((vals_h, vals_t, vals_r)):
                    series_dem[f"RDO {letra}"] = suma_elementos(vals_h, vals_t, vals_r)
        
            if series_dem:
                fig, ax = plt.subplots(figsize=(11, 5))
                yvals = []
                for nombre, valores in series_dem.items():
                    # Usa indices numéricos alineados a 'horas' (48 slots)
                    xlab, yv = recortar_ceros_inicio(valores, horas)
                    if not yv:
                        continue
                    start = len(horas) - len(yv)   # si recortaste al inicio
                    xnum = np.arange(start, start + len(yv))
                    yvals.extend(yv)
                    ax.plot(xnum, yv, marker="o", linewidth=2, label=nombre)
        
                # Formato unificado (X/Y) — esta primera define el "estándar"
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas, y_values=yvals, ypad=0.05, xpad=0.5)
        
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("DEMANDA"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); demanda_figs1.append(fig)
        
            # === ERRORES relativos absolutos ===
            try:
                if series_dem:
                    orden_series_dem = [
                        k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras])
                        if k in series_dem
                    ]
            
                    # pares consecutivos en orden temporal esperado
                    pares_dem = [
                        (orden_series_dem[i], orden_series_dem[i+1])
                        for i in range(len(orden_series_dem)-1)
                    ]
            
                    # 1) construimos cada curva original (sin cortar) con NaN ya aplicados a 0% y 100%
                    curvas = []  # lista de dicts: {label, y_full (np.array float, len L)}
                    L_global = None
            
                    for (ante, act) in pares_dem:
                        va = series_dem[ante]
                        vb = series_dem[act]
            
                        mL = min(len(va), len(vb), 48)
                        if mL <= 0:
                            continue
            
                        etiqueta = f"Error {ante} - {act}"
            
                        vals = [
                            _rel_err_abs_pct(va[i], vb[i]) for i in range(mL)
                        ]
            
                        # limpiamos 0 y 100 -> NaN
                        y_clean = np.array([_omit_0_100(v) for v in vals], dtype=float)
            
                        # guardo longitud base L_global = número de medias horas reales
                        if L_global is None:
                            L_global = mL
                        else:
                            L_global = min(L_global, mL)
            
                        curvas.append({
                            "label": etiqueta,
                            "y_clean": y_clean  # todavía sin recorte posterior
                        })
            
                    if curvas:
                        # alineamos todas al mismo largo L = L_global
                        L = L_global
                        x = np.arange(L)
            
                        # 2) para cada curva, detectamos su primer índice válido (donde realmente empieza)
                        for c in curvas:
                            ysub = c["y_clean"][:L]
                            not_nan = np.where(~np.isnan(ysub))[0]
                            if len(not_nan) == 0:
                                c["start_idx"] = None
                                c["end_idx"] = None
                            else:
                                c["start_idx"] = int(not_nan[0])
                                c["end_idx"]   = int(not_nan[-1])
                        
                        # 3) aplicamos la regla "cuando empieza la nueva, la anterior se apaga"        
                        y_final_list = []
                        for i, c in enumerate(curvas):
                            ymask = c["y_clean"][:L].copy() 
                            if i > 0:
                                # inicio de la curva i actual
                                s_new = curvas[i]["start_idx"]
                                if s_new is not None:
                                    for j in range(i):
                                        prev = y_final_list[j]
                                        prev[s_new:] = np.nan
            
                            y_final_list.append(ymask)
            
                        y_final_list = []
                        for i, c in enumerate(curvas):
                            ymask = c["y_clean"][:L].copy()
                            y_final_list.append(ymask)
            
                        # ahora aplicamos cortes acumulativos:
                        for i in range(1, len(curvas)):
                            s_new = curvas[i]["start_idx"]
                            if s_new is None:
                                continue
                            # cortar todas las anteriores desde s_new
                            for j in range(i):
                                y_final_list[j][s_new:] = np.nan
            
                        # 4) graficar ya con los cortes aplicados
                        fig, ax = plt.subplots(figsize=(11, 5))
                        ydata_all = []
            
                        for i, c in enumerate(curvas):
                            serie_plot = y_final_list[i]
                            # recolectar datos reales para escalar eje Y
                            ydata_all.extend(serie_plot[~np.isnan(serie_plot)])
            
                            ax.plot(
                                x,
                                serie_plot,
                                marker='o',
                                linewidth=2,
                                label=c["label"]
                            )
            
                        # 5) mantener tus ejes originales
                        aplicar_formato_xy(
                            ax,
                            L=L,
                            ticks_pos=ticks_pos,   # tus posiciones reales de las horas
                            horas=horas,           # tus etiquetas reales de las horas
                            y_values=ydata_all,
                            ypad=0.05,
                            xpad=0.5
                        )
            
                        ax.grid(axis="y", linestyle="--", alpha=0.5)
                        ax.set_title("Error Porcentual de DEMANDA")
                        ax.set_ylabel("%")
                        ax.legend()
                        plt.tight_layout()
                        demanda_figs1.append(fig)
            
            except Exception:
                pass
            
        except Exception:
            pass
        
        if demanda_figs1:
            cols = st.columns(len(demanda_figs1))
            for i, fig in enumerate(demanda_figs1):
                with cols[i]:
                    st.pyplot(fig)
                plt.close(fig)
    
    with tab2:
        # =========================================================
        # ================= MOTIVOS Y COSTOS ======================
        # =========================================================
        try:
            col_tabla, col_graf = st.columns([3, 2])  # ajusta proporciones 3:2 a tu gusto
        
            with col_tabla:
                df_motivos_vista = st.session_state.get("df_motivos")
                if df_motivos_vista is not None and not df_motivos_vista.empty:
                    st.markdown("### Motivo de Reprograma Diario")
                    st.dataframe(df_motivos_vista, width="stretch")
                    
            with col_graf:
                # ==================== 1) Recolectar PDO y RDOs ====================
                resultados_por_fuente = []
                fecha_str_local = f"{y}{m}{d}"  # ej. "20251019"
                
                # PDO baseline
                pdo_root = work_dir / f"PDO_{fecha_str_local}"
                sub_pdo, filas_pdo, df_pdo_full, path_pdo = procesar_raiz(pdo_root)
                if path_pdo is None:
                    df_pdo_full = None
                    filas_pdo = 0
            
                if path_pdo is not None:
                    resultados_por_fuente.append({
                        "nombre": "PDO",
                        "subtotal": sub_pdo,
                        "filas": filas_pdo,
                        "ruta": path_pdo,
                        "df": df_pdo_full,
                    })
            
                # RDOs
                for letra_rdo in rdo_letras:
                    rdo_root = work_dir / f"RDO_{letra_rdo}_{fecha_str_local}"
                    sub_rdo, filas_rdo, df_rdo_full, path_rdo = procesar_raiz(rdo_root)
                    if path_rdo is not None:
                        resultados_por_fuente.append({
                            "nombre": f"RDO {letra_rdo}",
                            "subtotal": sub_rdo,
                            "filas": filas_rdo,
                            "ruta": path_rdo,
                            "df": df_rdo_full,
                        })
            
                # ==================== 2) Armar datos de barra (ajustados) ====================
                etiquetas = []
                valores = []
                diffs_por_barra = []
            
                for item in resultados_por_fuente:
                    nombre   = item["nombre"]
                    subtotal = item["subtotal"]
                    filas    = item["filas"]
            
                    # diferencia de filas respecto al PDO (cuántas filas tiene PDO que este no tiene)
                    diff_filas = filas_pdo - filas
            
                    if nombre == "PDO":
                        # para PDO usamos tal cual
                        etiquetas.append(nombre)
                        valores.append(subtotal)
                    else:
                        # para cada RDO
                        if (diff_filas > 0) and (df_pdo_full is not None):
                            # agarrar las primeras diff_filas filas del PDO y sumar su costo
                            df_pdo_head = df_pdo_full.head(diff_filas)
                            extra_subtotal, _ = sumar_costo_termica_dataframe(df_pdo_head)
                            nuevo_total = extra_subtotal + subtotal
                            etiquetas.append(nombre)
                            valores.append(nuevo_total)
                            diffs_por_barra.append(diff_filas)
                        else:
                            # sin ajuste
                            etiquetas.append(nombre)
                            valores.append(subtotal)
                            diffs_por_barra.append(diff_filas)
                            
                labels_por_barra = [f"RDO {chr(65 + i)}" for i in range(len(diffs_por_barra))]
                # ==================== 3) Graficar ====================
                if valores:
                    fig, ax = plt.subplots(figsize=(8, 4))
                    ax.bar(etiquetas, valores)
            
                    ymax = max(valores) if valores else 0.0
                    ax.set_ylim(0, ymax * 1.15 if ymax > 0 else 1)
            
                    ax.set_ylabel("$")
                    ax.set_title("Costo Total del PDO y RDOs")
            
                    try:
                        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
                    except Exception:
                        pass
            
                    for x_, v in zip(etiquetas, valores):
                        ax.annotate(
                            f"{v:,.0f}",
                            xy=(x_, v),
                            xytext=(0, 3),
                            textcoords="offset points",
                            ha="center",
                            va="bottom",
                            fontsize=8,
                        )
            
                    plt.tight_layout()
                    st.pyplot(fig, width="stretch")
                    plt.close(fig)
                    
        except Exception:
            pass
        
        # =====================================================
        # ==================== ÍNDICES ========================
        # =====================================================
        try:
            st.markdown("### Índices")
        
            # Usa las marcas si existen; si no, listas vacías
            marcas_x  = diffs_por_barra if 'diffs_por_barra' in locals() else []
            marcas_lbl = labels_por_barra if 'labels_por_barra' in locals() else []
        
            def _plot_indices_streamlit(x_labels, y_vals, title, marcas_x=None, marcas_lbl=None):
                fig, ax = plt.subplots(figsize=(10, 4))
                xs = list(range(48))
                ax.plot(xs, y_vals, linewidth=2, marker='o')
        
                tick_pos = list(range(48))
                tick_lab = [x_labels[i] for i in tick_pos]
                ax.set_xticks(tick_pos)
                ax.set_xticklabels(tick_lab, rotation=90)
                for lbl in ax.get_xticklabels():
                    lbl.set_ha('center')
        
                plt.subplots_adjust(bottom=0.25)
                ax.set_xlim(0, 47)
                ax.grid(True, which="both", alpha=0.3)
                ax.set_title(title)
                ax.set_ylabel("Valor")
                ax.set_ylim(bottom=0)
        
                # Líneas rojas punteadas + etiquetas RDO
                if marcas_x:
                    for i, x_pos in enumerate(marcas_x):
                        if 0 <= x_pos < 48:
                            ax.axvline(x=x_pos, color='red', linestyle='--', linewidth=1)
                            lbl_txt = marcas_lbl[i] if (marcas_lbl and i < len(marcas_lbl)) else None
                            if lbl_txt:
                                y_top = ax.get_ylim()[1]
                                ax.text(
                                    x_pos + 0.3, y_top * 0.9, lbl_txt,
                                    color='red', fontsize=8, rotation=90,
                                    va='top', ha='left'
                                )
        
                plt.tight_layout()
                return fig
        
            res_idx = extraer_listas_alfa_beta_gamma_ultimo(y, m, d, M, work_dir)
            if res_idx.get("reprograma"):
                xlbls = _build_halfhour_labels()
                alfa  = _pad_or_trim_48(res_idx.get("alfa"))
                beta  = _pad_or_trim_48(res_idx.get("beta"))
                gamma = _pad_or_trim_48(res_idx.get("gamma"))
        
                c1, c2, c3 = st.columns(3)
                with c1:
                    fig = _plot_indices_streamlit(xlbls, alfa,  "ALFA (HIDRO)",   marcas_x, marcas_lbl)
                    st.pyplot(fig, width="stretch"); plt.close(fig)
                with c2:
                    fig = _plot_indices_streamlit(xlbls, beta,  "BETA (TERMO)",   marcas_x, marcas_lbl)
                    st.pyplot(fig, width="stretch"); plt.close(fig)
                with c3:
                    fig = _plot_indices_streamlit(xlbls, gamma, "GAMMA (DEMANDA)",marcas_x, marcas_lbl)
                    st.pyplot(fig, width="stretch"); plt.close(fig)
        except Exception:
            pass
        
    with tab3:
        # =========================================================
        # ==================== HIDRO Y ERROR ======================
        # =========================================================
        st.markdown("### HIDRO")
        hidro_figs1 = []
        
        try:
            barras_rer = ["CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                          "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                          "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                          "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"]
            stem_hidro = "Hidro - Despacho (MW)"
            stem_rer   = "Rer y No COES - Despacho (MW)"
        
            # === Series HIDRO (PDO + RDOs) ===
            series_h = {}
            df_pdo_h   = cargar_dataframe(pdo_res, stem_hidro)
            df_pdo_rer = cargar_dataframe(pdo_res, stem_rer)
            tot_hidro = rellenar_hasta_48(totales_hidro(df_pdo_h))
            tot_rer   = rellenar_hasta_48(totales_rer(df_pdo_rer, [x.upper() for x in barras_rer]))
            if tot_hidro and tot_rer:
                series_h["PDO"] = suma_elementos(tot_hidro, tot_rer)
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(rdo_res, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer (cargar_dataframe(rdo_res, stem_rer), [x.upper() for x in barras_rer]))
                if th and tr:
                    series_h[f"RDO {letra}"] = suma_elementos(th, tr)
        
            # === HIDRO (MW) — Línea ===
            if series_h:
                fig, ax = plt.subplots(figsize=(11, 5))
                yvals_h = []
                for nombre, valores in series_h.items():
                    xlab, yv = recortar_ceros_inicio(valores, horas)
                    if not yv:
                        continue
                    # Mapea a índices numéricos alineados a 'horas'
                    start = len(horas) - len(yv)
                    xnum = np.arange(start, start + len(yv))
                    yvals_h.extend(yv)
                    ax.plot(xnum, yv, marker="o", linewidth=2, label=nombre)
        
                # Formato unificado (igual que en DEMANDA)
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas,
                                   y_values=yvals_h, ypad=0.05, xpad=0.5)
        
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HIDRO"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); hidro_figs1.append(fig)
        
        except Exception:
            pass
        
        # ==================== ERROR HIDRO ======================
        try:
            if series_h:
                # orden esperado: ["PDO", "RDO A", "RDO B", ...] pero solo los que existen en series_h
                orden_series = [
                    k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras])
                    if k in series_h
                ]
        
                # pares consecutivos: ("PDO","RDO A"), ("RDO A","RDO B"), ...
                pares = [
                    (orden_series[i], orden_series[i+1])
                    for i in range(len(orden_series)-1)
                ]
        
                # 1) construir curvas de error individuales (sin cortar aún)
                curvas = []  # lista de dicts {label, y_clean}
                L_global = None
        
                for (ante, act) in pares:
                    va = series_h[ante]
                    vb = series_h[act]
        
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
        
                    etiqueta = f"Error {ante} - {act}"
        
                    # error porcentual punto a punto
                    vals = [
                        _rel_err_abs_pct(va[i], vb[i])
                        for i in range(mL)
                    ]
        
                    # limpiamos 0% / 100% => NaN (para no contaminar visual)
                    y_clean = np.array([_omit_0_100(v) for v in vals], dtype=float)
        
                    if L_global is None:
                        L_global = mL
                    else:
                        L_global = min(L_global, mL)
        
                    curvas.append({
                        "label": etiqueta,
                        "y_clean": y_clean
                    })
        
                if curvas:
                    # alineamos todas al mismo largo base
                    L = L_global
                    x = np.arange(L)
        
                    # 2) detectar para cada curva su primer índice válido (start_idx)
                    for c in curvas:
                        ysub = c["y_clean"][:L]
                        not_nan = np.where(~np.isnan(ysub))[0]
                        if len(not_nan) == 0:
                            c["start_idx"] = None
                            c["end_idx"] = None
                        else:
                            c["start_idx"] = int(not_nan[0])
                            c["end_idx"]   = int(not_nan[-1])
        
                    # 3) aplicar la lógica "apaga la anterior cuando arranca la nueva"
                    y_final_list = []
                    for c in curvas:
                        y_final_list.append(c["y_clean"][:L].copy())
        
                    for i in range(1, len(curvas)):
                        s_new = curvas[i]["start_idx"]
                        if s_new is None:
                            continue
                        for j in range(i):
                            y_final_list[j][s_new:] = np.nan
        
                    # 4) graficar resultado final
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata_e = []
        
                    for i, c in enumerate(curvas):
                        serie_plot = y_final_list[i]
                        ydata_e.extend(serie_plot[~np.isnan(serie_plot)])
        
                        ax.plot(
                            x,
                            serie_plot,
                            marker='o',
                            linewidth=2,
                            label=c["label"]
                        )
        
                    # Misma X que HIDRO (y DEMANDA) + aire en Y
                    aplicar_formato_xy(
                        ax,
                        L=L,
                        ticks_pos=ticks_pos,
                        horas=horas,
                        y_values=ydata_e,
                        ypad=0.05,
                        xpad=0.5
                    )
        
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de HIDRO")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    hidro_figs1.append(fig)
        
        except Exception:
            pass
        
        # Mostrar todas las HIDRO en UNA FILA
        if hidro_figs1:
            cols = st.columns(len(hidro_figs1))
            for i, fig in enumerate(hidro_figs1):
                with cols[i]:
                    st.pyplot(fig)
                plt.close(fig)
                
        # =========================================================
        # ==================== EÓLICA Y ERROR =====================
        # =========================================================
        st.markdown("### EÓLICA")
        eolica_figs1 = []
        
        try:
            stem_rer = "Rer y No COES - Despacho (MW)"
            barras_eol = [
                "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
                "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
                "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"
            ]
        
            # Series EÓLICA (PDO + RDOs)
            series_rer = {}
            df_pdo_rer = cargar_dataframe(pdo_res, stem_rer)
            vals_pdo   = rellenar_hasta_48(totales_rer(df_pdo_rer, [x.upper() for x in barras_eol]))
            if vals_pdo:
                series_rer["PDO"] = vals_pdo
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                df_rdo   = cargar_dataframe(rdo_res, stem_rer)
                vals_rdo = rellenar_hasta_48(totales_rer(df_rdo, [x.upper() for x in barras_eol]))
                if vals_rdo:
                    series_rer[f"RDO {letra}"] = vals_rdo
        
            # EÓLICA (MW) — Línea
            if series_rer:
                fig, ax = plt.subplots(figsize=(11, 5))
                y_plot = []
                for nombre, valores in series_rer.items():
                    xlab, yv = recortar_ceros_inicio(valores, horas)
                    if not yv:
                        continue
                    start = len(horas) - len(yv)         # alinear respecto a 'horas' (48 slots)
                    xnum  = np.arange(start, start + len(yv))
                    y_plot.extend(yv)
                    ax.plot(xnum, yv, marker="o", linewidth=2, label=nombre)
        
                # Formato unificado (misma X, margen en X y “aire” en Y)
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas,
                                   y_values=y_plot, ypad=0.05, xpad=0.5)
        
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("EÓLICO"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); eolica_figs1.append(fig)
            
        except Exception:
            pass 
        
        # ============== Error EÓLICA ============== 
        try:
            if series_rer:
                # Orden lógico: PDO, RDO A, RDO B, ... (solo los existentes)
                orden_series_eol = [
                    k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras])
                    if k in series_rer
                ]
        
                # Pares consecutivos: ("PDO","RDO A"), ("RDO A","RDO B"), ...
                pares_eol = [
                    (orden_series_eol[i], orden_series_eol[i+1])
                    for i in range(len(orden_series_eol)-1)
                ]
        
                # 1) construir curvas iniciales
                curvas = []  # [{label, y_clean}]
                L_global = None
        
                for (ante, act) in pares_eol:
                    va = series_rer[ante]
                    vb = series_rer[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
        
                    etiqueta = f"Error {ante} - {act}"
                    vals = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
                    y_clean = np.array([_omit_0_100(v) for v in vals], dtype=float)
        
                    if L_global is None:
                        L_global = mL
                    else:
                        L_global = min(L_global, mL)
        
                    curvas.append({
                        "label": etiqueta,
                        "y_clean": y_clean
                    })
        
                if curvas:
                    L = L_global
                    x = np.arange(L)
        
                    # 2) detectar inicio y fin reales de cada serie
                    for c in curvas:
                        ysub = c["y_clean"][:L]
                        not_nan = np.where(~np.isnan(ysub))[0]
                        if len(not_nan) == 0:
                            c["start_idx"] = None
                            c["end_idx"] = None
                        else:
                            c["start_idx"] = int(not_nan[0])
                            c["end_idx"]   = int(not_nan[-1])
        
                    # 3) apagar la anterior cuando arranca la nueva
                    y_final_list = [c["y_clean"][:L].copy() for c in curvas]
                    for i in range(1, len(curvas)):
                        s_new = curvas[i]["start_idx"]
                        if s_new is None:
                            continue
                        for j in range(i):
                            y_final_list[j][s_new:] = np.nan
        
                    # 4) graficar todo en un mismo eje
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata = []
        
                    for i, c in enumerate(curvas):
                        serie_plot = y_final_list[i]
                        ydata.extend(serie_plot[~np.isnan(serie_plot)])
                        ax.plot(
                            x,
                            serie_plot,
                            marker='o',
                            linewidth=2,
                            label=c["label"]
                        )
        
                    # formato de ejes idéntico al resto
                    aplicar_formato_xy(
                        ax,
                        L=L,
                        ticks_pos=ticks_pos,
                        horas=horas,
                        y_values=ydata,
                        ypad=0.05,
                        xpad=0.5
                    )
        
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de EÓLICO")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    eolica_figs1.append(fig)
        
        except Exception:
            pass
        
        # Mostrar EÓLICA en una fila
        if eolica_figs1:
            cols = st.columns(len(eolica_figs1))
            for i, fig in enumerate(eolica_figs1):
                with cols[i]:
                    st.pyplot(fig)
                plt.close(fig)
                
        # =========================================================
        # =================== SOLAR Y ERROR =======================
        # =========================================================
        st.markdown("### SOLAR")
        solar_figs1 = []
        try:
            stem_rer = "Rer y No COES - Despacho (MW)"
            barras_solar = [
                "MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
                "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
                "CS MATARANI","CS SAN MARTIN","CS SUNNY"
            ]
            # Actual
            series_sol = {}
            df_pdo_sol = cargar_dataframe(pdo_res, stem_rer)
            vals_pdo   = rellenar_hasta_48(totales_rer(df_pdo_sol, [x.upper() for x in barras_solar]))
            if vals_pdo:
                series_sol["PDO"] = vals_pdo
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                df_rdo_sol = cargar_dataframe(rdo_res, stem_rer)
                vals_rdo   = rellenar_hasta_48(totales_rer(df_rdo_sol, [x.upper() for x in barras_solar]))
                if vals_rdo and any(v != 0 for v in vals_rdo):
                    series_sol[f"RDO {letra}"] = vals_rdo
        
            if series_sol:
                fig, ax = plt.subplots(figsize=(11, 5))
                y_plot = []
                for nombre, raw_vals in series_sol.items():
                    # Construye 48 puntos con el enmascaramiento pedido:
                    # if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47) -> ocultar (None)
                    y_vals = []
                    for i, v in enumerate(raw_vals[:48]):
                        v = 0 if pd.isna(v) else v
                        if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                            y_vals.append(None)
                        else:
                            y_vals.append(v)
        
                    if all(v is None for v in y_vals):
                        continue
        
                    # X numérica alineada a 'horas'
                    xnum = np.arange(len(horas))  # 0..47
                    # Para calcular márgenes Y, solo valores válidos
                    y_plot.extend([v for v in y_vals if v is not None])
        
                    ax.plot(xnum, y_vals, marker="o", linewidth=2, label=nombre)
        
                # Formato unificado (misma X y “aire” en Y)
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas,
                                   y_values=y_plot, ypad=0.05, xpad=0.5)
        
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("SOLAR"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); solar_figs1.append(fig)
        
        except Exception:
            pass
                
        
        # Error Solar
        try:
            if series_sol:
                # Armamos el orden lógico: PDO, RDO A, RDO B, ... (solo los que existen)
                orden_series_sol = [
                    k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras])
                    if k in series_sol
                ]
        
                # Pares consecutivos para comparar
                pares_sol = [
                    (orden_series_sol[i], orden_series_sol[i+1])
                    for i in range(len(orden_series_sol)-1)
                ]
        
                # errores_sol["Error PDO-RDO A"] = [...]
                errores_sol = {}
                for (ante, act) in pares_sol:
                    va, vb = series_sol[ante], series_sol[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
        
                    etiqueta = f"Error {ante} - {act}"
                    errores_sol[etiqueta] = [
                        _rel_err_abs_pct(va[i], vb[i]) for i in range(mL)
                    ]
        
                if errores_sol:
                    L = min(len(v) for v in errores_sol.values() if v) or len(horas)
                    x = np.arange(L)
        
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata = []
        
                    # ploteamos en el mismo orden lógico PDO→RDO A→RDO B→...
                    for (ante, act) in pares_sol:
                        etiqueta = f"Error {ante} - {act}"
                        if etiqueta not in errores_sol:
                            continue
        
                        serie_vals = errores_sol[etiqueta]
                        serie = np.array(
                            [_omit_0_100(v) for v in serie_vals[:L]],
                            dtype=float
                        )
        
                        ydata.extend(serie[~np.isnan(serie)])
                        ax.plot(
                            x,
                            serie,
                            marker='o',
                            linewidth=2,
                            label=etiqueta
                        )
        
                    # Misma X que el gráfico SOLAR + “aire” en Y
                    aplicar_formato_xy(
                        ax,
                        L=L,
                        ticks_pos=ticks_pos,
                        horas=horas,
                        y_values=ydata,
                        ypad=0.05,
                        xpad=0.5
                    )
        
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de SOLAR")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    solar_figs1.append(fig)
        
        except Exception:
            pass
        
        if solar_figs1:
            cols = st.columns(len(solar_figs1))
            for i, fig in enumerate(solar_figs1):
                with cols[i]:
                    st.pyplot(fig)
                plt.close(fig)
    
    with tab4:
        # =========================================================
        # ======================== CMG ============================
        # =========================================================
        try:
            st.markdown("### CMG")
            stem_file = "CMg - Barra ($ por MWh)"
            df_pdo = cargar_dataframe(pdo_res, stem_file)
    
            for barra in barras:
                datosPDO = rellenar_hasta_48(extraer_columna(df_pdo, barra))
                if not datosPDO:
                    continue
    
                series_barra = {"PDO": datosPDO}
    
                # cargo todos los RDO para esa barra
                for letra in rdo_letras:
                    rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                    df_rdo = cargar_dataframe(rdo_res, stem_file)
                    datosRDO = rellenar_hasta_48(extraer_columna(df_rdo, barra))
                    if datosRDO:
                        series_barra[f"RDO {letra}"] = datosRDO
    
                # ploteo
                fig, ax = plt.subplots(figsize=(11, 5))
                ok = _plot_cmg_barra_en_axes(ax, barra, series_barra, horas, ticks_pos, ticks_lbl)
                if ok:
                    plt.tight_layout()
                    st.pyplot(fig)
                plt.close(fig)
    
        except Exception:
            pass
        
    with tab5:
        # =========================================================
        # ==================== HISTORICO HIDRO ====================
        # =========================================================
        st.markdown("### HIDRO")
        
        # Histórico IEOD (HIDRO)
        try:    
            series_por_dia = []
            dias = (fin - ini).days + 1
            for k in range(dias):
                f = ini + timedelta(days=k)
                fb = _lee_ieod_bytes(f.year, f.month, MES_TXT[f.month-1], f.day)
                df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                c_pas, c_reg = _find_cols_ieod(df)
                if not c_pas or not c_reg:
                    continue
                sub = df.iloc[0:48, :]
                pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                v_sum = [pas[i] + reg[i] for i in range(48)]
                series_por_dia.append((f.strftime("%Y-%m-%d"), v_sum))
        
            # Si quisieras mostrar el histórico IEOD por separado,
            # aquí podrías armar otra figura (matplotlib o plotly).
        except Exception as e:
            st.warning(f"IEOD histórico no disponible: {e}")
        
        # Histórico HIDRO de RPO A
        try:
            series_dia = {}
            dias = (fin - ini).days + 1
            stem_hidro = "Hidro - Despacho (MW)"
            stem_rer   = "Rer y No COES - Despacho (MW)"
            barras_rer_up = [
                "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"
            ]
            for k in range(dias):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
                M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                            zf.extractall(path=carpeta)
                    except Exception:
                        continue
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer(cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
                if th and tr:
                    series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
        except Exception:
            pass
        
        # Fusión (IEOD + RPO A último día) con Plotly interactivo
        try:
            series_7 = {}
            stem_hidro = "Hidro - Despacho (MW)"
            stem_rer   = "Rer y No COES - Despacho (MW)"
            barras_rer_up = [
                "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"
            ]
        
            # IEOD: desde ini hasta fin-1
            ini_ieod = ini
            fin_ieod = fin - timedelta(days=1)
            dias_ieod = (fin_ieod - ini_ieod).days + 1 if fin_ieod >= ini_ieod else 0
        
            for k in range(dias_ieod):
                f = ini_ieod + timedelta(days=k)
                y2, m2, d2 = f.year, f.month, f.day
                M2 = MES_TXT[m2-1]
                try:
                    fb = _lee_ieod_bytes(y2, m2, M2, d2)
        
                    def _find_cols(df):
                        def _n(s):
                            import re
                            return re.sub(r"\s+"," ",str(s).strip()).upper()
                        c_pas = c_reg = None
                        for c in df.columns:
                            k = _n(c)
                            if k == "H. PASADA" and c_pas is None:
                                c_pas = c
                            if k == "H. REGULACION" and c_reg is None:
                                c_reg = c
                        return c_pas, c_reg
        
                    df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                    c_pas, c_reg = _find_cols(df)
                    if c_pas and c_reg:
                        sub = df.iloc[0:48, :]
                        pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()
                        reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()
                        vals = [pas[i] + reg[i] for i in range(48)]
                        series_7[f.strftime("%Y-%m-%d")] = vals
                except Exception:
                    continue
        
            # RDO-A último día (fin)
            f_last = fin
            yk, mk, dk = f_last.year, f_last.strftime("%m"), f_last.strftime("%d")
            M_TXT = MES_TXT[f_last.month-1]
            carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
            resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
            if not resultados.exists():
                try:
                    r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40)
                    r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                        zf.extractall(path=carpeta)
                except Exception:
                    pass
        
            th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
            tr = rellenar_hasta_48(totales_rer(cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
            if th and tr:
                series_7[f_last.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
        
            # Plot interactivo
            if series_7:
                fechas_orden = []
                cur = ini
                while cur <= fin:
                    lbl = cur.strftime("%Y-%m-%d")
                    if lbl in series_7:
                        fechas_orden.append(lbl)
                    cur += timedelta(days=1)
        
                fig = go.Figure()
                x_idx = list(range(48))
                y_all = []
        
                for lbl in fechas_orden:
                    fobj = datetime.strptime(lbl, "%Y-%m-%d").date()
                    vals = series_7[lbl]
                    y_all.extend(vals)
        
                    if fobj == fin:
                        # Día actual → rojo, más grueso
                        fig.add_trace(go.Scatter(
                            x=x_idx,
                            y=vals,
                            mode='lines+markers',
                            name=f"{lbl}",
                            line=dict(color='red', width=4)
                        ))
                    else:
                        estilo = 'dash' if fobj < fin else 'solid'
                        fig.add_trace(go.Scatter(
                            x=x_idx,
                            y=vals,
                            mode='lines+markers',
                            name=lbl,
                            line=dict(width=2, dash=estilo)
                        ))
        
                if y_all:
                    y_min = max(0, math.floor(min(y_all)) - 10)
                    y_max = math.ceil(max(y_all)) + 10
                    fig.update_yaxes(range=[y_min, y_max])
        
                fig.update_layout(
                    # xaxis_title="Hora",
                    yaxis_title="MW",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=ticks_pos,
                        ticktext=ticks_lbl,
                        tickangle=0
                    ),
                    hovermode="x unified",
                    margin=dict(t=40, b=40, l=60, r=20)
                )
        
                st.plotly_chart(fig, use_container_width=True)
        except Exception:
            pass
        
        # =========================================================
        # ================== HISTORICO DEMANDA ====================
        # =========================================================
        st.markdown("### DEMANDA")
        demanda_figs2 = []
        
        try:
            # ------------ HISTÓRICO IEOD ------------
            series_ieod_dem = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_demanda_48(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_dem[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            # ------------ HISTÓRICO RPO A ------------
            series_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
                M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
        
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40)
                        r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                            zf.extractall(path=carpeta)
                    except Exception:
                        continue
        
                vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["HIDRO"])))
                vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["TERMICA"])))
                vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["RER"])))
        
                if any((vals_h, vals_t, vals_r)):
                    series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(vals_h, vals_t, vals_r)
        
            # ------------ FUSIÓN IEOD + RPO A (último día) ------------
            series_dem_7 = {}
            cur = ini
        
            # Añadir días IEOD (todos menos el último)
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_dem:
                    series_dem_7[lbl] = series_ieod_dem[lbl][:48]
                else:
                    try:
                        fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_demanda_48(fb)
                        if vals:
                            series_dem_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
        
            # Añadir último día RPO A
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_dia:
                series_dem_7[lbl_fin] = series_dia[lbl_fin][:48]
        
            # ------------ PLOTLY INTERACTIVO (FUSIÓN) ------------
            if series_dem_7:
        
                # ordenar fechas
                fechas_orden = []
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_dem_7:
                        fechas_orden.append(l)
                    cur += timedelta(days=1)
        
                fig = go.Figure()
                xs = list(range(48))
                y_all = []
        
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    vals = [
                        0 if (v is None or (isinstance(v, float) and math.isnan(v))) else v
                        for v in series_dem_7[l][:48]
                    ]
                    y_all.extend(vals)
        
                    if fobj == fin:
                        # Último día → rojo y grueso
                        fig.add_trace(go.Scatter(
                            x=xs,
                            y=vals,
                            mode='lines+markers',
                            name=f"{l}",
                            line=dict(color='red', width=4)
                        ))
                    else:
                        estilo = 'dash' if fobj < fin else 'solid'
                        fig.add_trace(go.Scatter(
                            x=xs,
                            y=vals,
                            mode='lines+markers',
                            name=l,
                            line=dict(width=2, dash=estilo)
                        ))
        
                # Rango dinámico de eje Y
                if y_all:
                    y_min = max(0, math.floor(min(y_all)) - 10)
                    y_max = math.ceil(max(y_all)) + 10
                    fig.update_yaxes(range=[y_min, y_max])
        
                fig.update_layout(
                    # xaxis_title="Hora",
                    yaxis_title="MW",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=ticks_pos,
                        ticktext=ticks_lbl,
                        tickangle=0
                    ),
                    hovermode="x unified",
                    margin=dict(t=40, b=40, l=60, r=20)
                )
        
                st.plotly_chart(fig, use_container_width=True)
        
        except Exception:
            pass
        
        # =========================================================
        # ==================== HISTORICO EÓLICO ====================
        # =========================================================
        st.markdown("### EÓLICA")
        
        try:
            # ------------ HISTÓRICO IEOD ------------
            def _extrae_eolica_48(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_eolica = None
                for c in df.columns:
                    if isinstance(c, str):
                        c_norm = _sin_acentos(c).upper().strip()
                        if "EOLICA" in c_norm:
                            col_eolica = c
                            break
                if not col_eolica:
                    return None
                vals = pd.to_numeric(df[col_eolica].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
        
            series_ieod_eol = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_eolica_48(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_eol[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            # ------------ HISTÓRICO RPO A ------------
            series_eol_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
                M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
        
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
        
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40)
                        r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                            zf.extractall(path=carpeta)
                    except:
                        continue
        
                df_rer = cargar_dataframe(resultados, stem_rer)
                tot_eol = rellenar_hasta_48(totales_rer(df_rer, [x.upper() for x in barras_eol]))
                if tot_eol:
                    series_eol_dia[f.strftime("%Y-%m-%d")] = tot_eol
        
            # ------------ FUSIÓN IEOD + RPO A (último día) ------------
            series_eol_7 = {}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_eol:
                    series_eol_7[lbl] = series_ieod_eol[lbl][:48]
                else:
                    try:
                        fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_eolica_48(fb)
                        if vals:
                            series_eol_7[lbl] = vals[:48]
                    except:
                        pass
                cur += timedelta(days=1)
        
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_eol_dia:
                series_eol_7[lbl_fin] = series_eol_dia[lbl_fin][:48]
        
            # ------------ PLOTLY INTERACTIVO (FUSIÓN EÓLICA) ------------
            if series_eol_7:
        
                # Ordenar fechas
                fechas_orden = []
                cur = ini
                while cur <= fin:
                    lbl = cur.strftime("%Y-%m-%d")
                    if lbl in series_eol_7:
                        fechas_orden.append(lbl)
                    cur += timedelta(days=1)
        
                fig = go.Figure()
                xs = list(range(48))
                y_all = []
        
                for lbl in fechas_orden:
                    fobj = datetime.strptime(lbl, "%Y-%m-%d").date()
                    vals = [
                        0 if (v is None or (isinstance(v, float) and math.isnan(v))) else v
                        for v in series_eol_7[lbl][:48]
                    ]
        
                    y_all.extend(vals)
        
                    if fobj == fin:
                        # Último día → rojo y grueso
                        fig.add_trace(go.Scatter(
                            x=xs,
                            y=vals,
                            mode='lines+markers',
                            name=f"{lbl}",
                            line=dict(color='red', width=4)
                        ))
                    else:
                        estilo = 'dash' if fobj < fin else 'solid'
                        fig.add_trace(go.Scatter(
                            x=xs,
                            y=vals,
                            mode='lines+markers',
                            name=lbl,
                            line=dict(width=2, dash=estilo)
                        ))
                        
                # Rango dinámico eje Y
                if y_all:
                    y_min = max(0, math.floor(min(y_all)) - 10)
                    y_max = math.ceil(max(y_all)) + 10
                    fig.update_yaxes(range=[y_min, y_max])
        
                fig.update_layout(
                    # xaxis_title="Hora",
                    yaxis_title="MW",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=ticks_pos,
                        ticktext=ticks_lbl,
                        tickangle=0
                    ),
                    hovermode="x unified",
                    margin=dict(t=40, b=40, l=60, r=20)
                )
        
                st.plotly_chart(fig, use_container_width=True)
        
        except Exception as e:
            st.warning(f"Error en histórico eólico: {e}")
            
        # =========================================================
        # ==================== HISTORICO SOLAR ====================
        # =========================================================
        st.markdown("### SOLAR")
        
        try:
            # ------------ HISTÓRICO IEOD ------------
            def _extrae_solar_48_s(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_solar = None
                for c in df.columns:
                    if isinstance(c, str) and "SOLAR" in c.upper():
                        col_solar = c
                        break
                if not col_solar:
                    return None
                vals = pd.to_numeric(df[col_solar].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
        
            series_ieod_solar = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_solar_48_s(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_solar[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            # ------------ HISTÓRICO RPO A ------------
            series_sol_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
                M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
        
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
        
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40)
                        r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf:
                            zf.extractall(path=carpeta)
                    except Exception:
                        continue
        
                df_sol = cargar_dataframe(resultados, stem_rer)
                vals   = rellenar_hasta_48(totales_rer(df_sol, [x.upper() for x in barras_solar]))
                if vals and any(v != 0 for v in vals):
                    series_sol_dia[f.strftime("%Y-%m-%d")] = vals
        
            # ------------ FUSIÓN IEOD + RPO A (último día) ------------
            series_solar_7 = {}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_solar:
                    series_solar_7[lbl] = series_ieod_solar[lbl][:48]
                else:
                    try:
                        fb   = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_solar_48_s(fb)
                        if vals:
                            series_solar_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
        
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_sol_dia:
                series_solar_7[lbl_fin] = series_sol_dia[lbl_fin][:48]
        
            # ------------ PLOTLY INTERACTIVO (RESPETANDO LÓGICA DE CEROS) ------------
            if series_solar_7:
        
                # ordenar fechas válidas
                fechas_orden = []
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_solar_7:
                        fechas_orden.append(l)
                    cur += timedelta(days=1)
        
                fig = go.Figure()
                xs = list(range(48))
                y_all = []
        
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    raw_vals = series_solar_7[l][:48]
                    y_vals = []
        
                    for i, v in enumerate(raw_vals):
                        # limpieza básica
                        if v is None or (isinstance(v, float) and math.isnan(v)):
                            v0 = 0
                        else:
                            v0 = v
        
                        # MISMA LÓGICA QUE TU CÓDIGO ORIGINAL:
                        # - 0 en madrugada/noche (0–11 y 36–47) se muestra como 0
                        # - 0 en horas centrales -> None (no se dibuja línea)
                        if v0 == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                            y_vals.append(None)
                        else:
                            y_vals.append(v0)
        
                    y_all.extend([vv for vv in y_vals if vv is not None])
        
                    if fobj == fin:
                        # Última fecha: línea destacada
                        fig.add_trace(go.Scatter(
                            x=xs,
                            y=y_vals,
                            mode='lines+markers',
                            name=f"{l}",
                            line=dict(color='red', width=4)
                        ))
                    else:
                        estilo = 'dash' if fobj < fin else 'solid'
                        fig.add_trace(go.Scatter(
                            x=xs,
                            y=y_vals,
                            mode='lines+markers',
                            name=l,
                            line=dict(width=2, dash=estilo)
                        ))
        
                # Rango eje Y dinámico
                if y_all:
                    y_min = max(0, math.floor(min(y_all)) - 10)
                    y_max = math.ceil(max(y_all)) + 10
                    fig.update_yaxes(range=[y_min, y_max])
        
                fig.update_layout(
                    # xaxis_title="Hora",
                    yaxis_title="MW",
                    xaxis=dict(
                        tickmode='array',
                        tickvals=ticks_pos,
                        ticktext=ticks_lbl,
                        tickangle=0
                    ),
                    hovermode="x unified",
                    margin=dict(t=40, b=40, l=60, r=20)
                )
        
                st.plotly_chart(fig, use_container_width=True)
        
        except Exception as e:
            st.warning(f"Error en histórico solar: {e}")
        
    with tab6:    
        # =========================================================
        # ======================== HIDRO ==========================
        # =========================================================
        st.markdown("### HIDRO")
        hidro_figs = []
    
        # H. Pasada vs H. Regulación
        try:
            def _norm(txt):
                import re; return re.sub(r"\s+"," ",str(txt).strip()).upper()
            def _find_cols(cols):
                c_pas = c_reg = None
                for c in cols:
                    k = _norm(c)
                    if k == "H. PASADA" and c_pas is None: c_pas = c
                    if k == "H. REGULACION" and c_reg is None: c_reg = c
                return c_pas, c_reg
            def _extrae_listas_48(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                c_pas, c_reg = _find_cols(df.columns)
                if not c_pas or not c_reg: return None, None, None
                sub = df.iloc[0:48, :]
                pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()
                reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()
                pas = (pas + [0.0]*48)[:48]; reg = (reg + [0.0]*48)[:48]
                return pas, reg, [pas[i]+reg[i] for i in range(48)]
    
            f = fin; y2, m2, d2 = f.year, f.month, f.day; M2 = MES_TXT[m2-1]
            fb = _lee_ieod_bytes(y2, m2, M2, d2)
            v_pas, v_reg, v_sum = _extrae_listas_48(fb)
            if v_sum is not None:
                fig, ax = plt.subplots(figsize=(9, 5))
                xs = list(range(48))
                
                ax.bar(xs, v_pas, label="H. PASADA")
                ax.bar(xs, v_reg, bottom=v_pas, label="H. REGULACION")
                
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                ax.set_title("H. PASADA - H. REGULACIÓN"); ax.set_ylabel("MW")
                ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(); plt.tight_layout()
                
                hidro_figs.append(fig)
        except Exception:
            pass
        
        # Histórico IEOD (HIDRO)
        try:    
            series_por_dia = []
            dias = (fin - ini).days + 1
            for k in range(dias):
                f = ini + timedelta(days=k)
                fb = _lee_ieod_bytes(f.year, f.month, MES_TXT[f.month-1], f.day)
                df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                c_pas, c_reg = _find_cols_ieod(df)
                if not c_pas or not c_reg:
                    continue
                sub = df.iloc[0:48, :]
                pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                v_sum = [pas[i] + reg[i] for i in range(48)]
                series_por_dia.append((f.strftime("%Y-%m-%d"), v_sum))
        except Exception as e:
            st.warning(f"IEOD histórico no disponible: {e}")
            
        # Histórico HIDRO de RPO A
        try:
            series_dia={}; dias=(fin-ini).days+1
            stem_hidro = "Hidro - Despacho (MW)"; stem_rer="Rer y No COES - Despacho (MW)"
            barras_rer_up = ["CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                             "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                             "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                             "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"]
            for k in range(dias):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
                if th and tr: series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
        except Exception:
            pass
        
        # Fusión + Promedio + Máximo
        try:
            series_7={}
            stem_hidro = "Hidro - Despacho (MW)"; stem_rer="Rer y No COES - Despacho (MW)"
            barras_rer_up = ["CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                             "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                             "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                             "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"]
            ini_ieod = ini; fin_ieod = fin - timedelta(days=1)
            dias_ieod = (fin_ieod - ini_ieod).days + 1 if fin_ieod >= ini_ieod else 0
            # IEOD
            for k in range(dias_ieod):
                f = ini_ieod + timedelta(days=k); y2, m2, d2 = f.year, f.month, f.day; M2 = MES_TXT[m2-1]
                try:
                    fb = _lee_ieod_bytes(y2, m2, M2, d2)
                    # usar extractor anterior de pasada/regulación
                    def _find_cols(df):
                        def _n(s): import re; return re.sub(r"\s+"," ",str(s).strip()).upper()
                        c_pas=c_reg=None
                        for c in df.columns:
                            k=_n(c)
                            if k=="H. PASADA" and c_pas is None: c_pas=c
                            if k=="H. REGULACION" and c_reg is None: c_reg=c
                        return c_pas, c_reg
                    df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                    c_pas, c_reg = _find_cols(df)
                    if c_pas and c_reg:
                        sub = df.iloc[0:48, :]
                        pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()
                        reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()
                        vals = [pas[i]+reg[i] for i in range(48)]
                        series_7[f.strftime("%Y-%m-%d")] = vals
                except Exception:
                    continue
            # RDO-A último día
            f_last = fin; yk, mk, dk = f_last.year, f_last.strftime("%m"), f_last.strftime("%d"); M_TXT = MES_TXT[f_last.month-1]
            carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"; resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
            if not resultados.exists():
                try:
                    r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                except Exception: pass
            th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
            tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
            if th and tr: series_7[f_last.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
    
            if series_7:
                # Fusión líneas
                fechas_orden=[]; cur=ini
                while cur<=fin:
                    lbl=cur.strftime("%Y-%m-%d")
                    if lbl in series_7: fechas_orden.append(lbl)
                    cur+=timedelta(days=1)
                fig, ax = plt.subplots(figsize=(12,6)); x_idx=list(range(48)); y_all=[]
                for lbl in fechas_orden:
                    fobj = datetime.strptime(lbl, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = series_7[lbl]
                    ax.plot(x_idx, vals, marker="o", linewidth=2, linestyle=estilo, label=lbl)
                    y_all.extend(vals)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    y_min = max(0, math.floor(min(y_all)) - 10); y_max = math.ceil(max(y_all)) + 10
                    ax.set_ylim(y_min, y_max)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO HIDRO"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout()
                plt.close(fig)
                
                # Promedio diario
                fechas_lbl=[]; promedios=[]
                cur=ini
                while cur<=fin:
                    lbl=cur.strftime("%Y-%m-%d")
                    if lbl in series_7:
                        vals=series_7[lbl][:48]
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in vals]
                        promedios.append(sum(vals)/len(vals)); fechas_lbl.append(lbl)
                    cur+=timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars=ax.bar(fechas_lbl, promedios)
                    for rect, val in zip(bars, promedios):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height()+1, f"{val:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW")
                    ax.set_title("HISTÓRICO HIDRO (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); hidro_figs.append(fig)
    
                # Máximo diario (Σ/2)
                fechas_lbl=[]; maximos=[]
                cur=ini
                while cur<=fin:
                    lbl=cur.strftime("%Y-%m-%d")
                    if lbl in series_7:
                        vals=series_7[lbl][:48]
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in vals]
                        maximos.append(sum(vals)/2.0); fechas_lbl.append(lbl)
                    cur+=timedelta(days=1)
                if maximos:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars=ax.bar(fechas_lbl, maximos)
                    for rect, val in zip(bars, maximos):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height()+1, f"{val:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MWh")
                    ax.set_title("HISTÓRICO HIDRO (Energía Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); hidro_figs.append(fig)
        except Exception:
            pass
    
        # Mostrar todas las HIDRO en UNA FILA
        if hidro_figs:
            cols = st.columns(len(hidro_figs))
            for i, fig in enumerate(hidro_figs):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
    
        # =========================================================
        # ======================== DEMANDA ========================
        # =========================================================
        st.markdown("### DEMANDA")
        demanda_figs = []
        try:
            # HISTÓRICO IEOD
            series_ieod_dem = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_demanda_48(fb)
                    if vals and any(v != 0 for v in vals): series_ieod_dem[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
            
            # HISTÓRICO RPO A
            series_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["HIDRO"])))
                vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["TERMICA"])))
                vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["RER"])))
                if any((vals_h, vals_t, vals_r)):
                    series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(vals_h, vals_t, vals_r)
    
            # Fusión, Promedio, Máximo
            series_dem_7={}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_dem:
                    series_dem_7[lbl] = series_ieod_dem[lbl][:48]
                else:
                    try:
                        fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_demanda_48(fb)
                        if vals: series_dem_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_dia: series_dem_7[lbl_fin] = series_dia[lbl_fin][:48]
    
            if series_dem_7:
                fechas_orden=[]; cur=ini
                while cur<=fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_dem_7: fechas_orden.append(l)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(12, 6)); xs=list(range(48)); y_all=[]
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_dem_7[l][:48]]
                    y_all.extend(vals)
                    ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO DEMANDA"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout()
                plt.close(fig)
    
                # Promedio
                fechas_lbl=[]; promedios=[]
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_dem_7:
                        vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v)
                                for v in series_dem_7[l][:48]]
                        promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                    cur += timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars = ax.bar(fechas_lbl, promedios)
                    for rect, val in zip(bars, promedios):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                                ha="center", va="bottom", fontsize=9, clip_on=True)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO DEMANDA (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); demanda_figs.append(fig)
    
                # Máximo diario
                fechas_lbl=[]; maximos=[]
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_dem_7:
                        vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v)
                                for v in series_dem_7[l][:48]]
                        maximos.append(max(vals) if vals else 0.0); fechas_lbl.append(l)
                    cur += timedelta(days=1)
                if maximos:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars = ax.bar(fechas_lbl, maximos)
                    for rect, val in zip(bars, maximos):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                                ha="center", va="bottom", fontsize=9, clip_on=True)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO DEMANDA (Máxima Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); demanda_figs.append(fig)
        except Exception:
            pass
    
        if demanda_figs:
            cols = st.columns(len(demanda_figs))
            for i, fig in enumerate(demanda_figs):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
    
        # =========================================================
        # ======================== EÓLICA =========================
        # =========================================================
        st.markdown("### EÓLICA")
        eolica_figs = []
        try:
            # HISTÓRICO IEOD
            def _extrae_eolica_48(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_eolica = None
                for c in df.columns:
                    if isinstance(c, str):
                        c_norm = _sin_acentos(c).upper().strip()
                        if "EOLICA" in c_norm:
                            col_eolica = c; break
                if not col_eolica: return None
                vals = pd.to_numeric(df[col_eolica].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
    
            series_ieod_eol = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_eolica_48(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_eol[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
    
            # HISTÓRICO RPO A
            series_eol_dia={}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                df_rer = cargar_dataframe(resultados, stem_rer)
                tot_eol = rellenar_hasta_48(totales_rer(df_rer, [x.upper() for x in barras_eol]))
                if tot_eol: series_eol_dia[f.strftime("%Y-%m-%d")] = tot_eol
    
            # Fusión + Promedio + Máximo
            series_eol_7={}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_eol:
                    series_eol_7[lbl] = series_ieod_eol[lbl][:48]
                else:
                    try:
                        fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_eolica_48(fb)
                        if vals: series_eol_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_eol_dia: series_eol_7[lbl_fin] = series_eol_dia[lbl_fin][:48]
            if series_eol_7:
                fechas_orden=[]; cur=ini
                while cur<=fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_eol_7: fechas_orden.append(l)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(12, 6)); xs=list(range(48)); y_all=[]
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_eol_7[l][:48]]
                    y_all.extend(vals); ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO EÓLICO"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout()
                plt.close(fig)
    
                # Promedio
                fechas_lbl=[]; promedios=[]
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_eol_7:
                        vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_eol_7[l][:48]]
                        promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                    cur += timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars = ax.bar(fechas_lbl, promedios)
                    for rect, val in zip(bars, promedios):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW")
                    ax.set_title("HISTÓRICO EÓLICO (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); eolica_figs.append(fig)
    
                # HISTÓRICO EÓLICO – PROMEDIO DIARIO (Norte/Centro)
                try:
                    N_INTERVALOS = 48
    
                    def _norm_txt(s: str) -> str:
                        return unicodedata.normalize("NFKD", str(s)).encode("ASCII","ignore").decode().upper().strip()
    
                    def _extrae_eolica_ns_gareas(fbytes):
                        df = pd.read_excel(fbytes, sheet_name="G_AREAS", header=None, engine="openpyxl")
                        fila_rot = col_norte = col_centro = None
                        for i in df.index:
                            for j in df.columns:
                                v = _norm_txt(df.iat[i, j])
                                if "GENERACION EOLICA" in v:
                                    if "NORTE" in v:  fila_rot, col_norte  = i, j
                                    if "CENTRO" in v: fila_rot = i if fila_rot is None else fila_rot; col_centro = j
                            if fila_rot is not None and col_norte is not None and col_centro is not None:
                                break
                        if fila_rot is None or col_norte is None or col_centro is None:
                            return None, None
    
                        v_norte  = pd.to_numeric(df.iloc[fila_rot+1:fila_rot+1+N_INTERVALOS, col_norte ],
                                                 errors="coerce").fillna(0.0).astype(float).tolist()
                        v_centro = pd.to_numeric(df.iloc[fila_rot+1:fila_rot+1+N_INTERVALOS, col_centro],
                                                 errors="coerce").fillna(0.0).astype(float).tolist()
                        v_norte  = (v_norte  + [0.0]*N_INTERVALOS)[:N_INTERVALOS]
                        v_centro = (v_centro + [0.0]*N_INTERVALOS)[:N_INTERVALOS]
                        return v_norte, v_centro
    
                    # Promedios diarios IEOD (ini → fin-1)
                    fechas, prom_norte, prom_centro = [], [], []
                    cur = ini
                    while cur < fin:
                        try:
                            fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                            vn, vc = _extrae_eolica_ns_gareas(fb)
                            if vn is not None and vc is not None:
                                fechas.append(cur.strftime("%Y-%m-%d"))
                                prom_norte.append(sum(vn)/N_INTERVALOS)
                                prom_centro.append(sum(vc)/N_INTERVALOS)
                        except Exception:
                            pass
                        cur += timedelta(days=1)
    
                    # Último día (fin) con RDO-A: dividir por barras (Norte/Centro)
                    try:
                        f = fin
                        yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
                        M_TXT = MES_TXT[f.month-1]
                        carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                        resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                        if not resultados.exists():
                            r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                            with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
    
                        df_rer  = cargar_dataframe(resultados, stem_rer)
                        NORTE   = {"PE TALARA","PE CUPISNIQUE","HUAMBOS","DUNA"}
                        CENTRO  = set(x.upper() for x in barras_eol) - NORTE
    
                        vn = rellenar_hasta_48(totales_rer(df_rer, list(NORTE)))  or [0.0]*N_INTERVALOS
                        vc = rellenar_hasta_48(totales_rer(df_rer, list(CENTRO))) or [0.0]*N_INTERVALOS
    
                        fechas.append(f.strftime("%Y-%m-%d"))
                        prom_norte.append(sum(vn)/N_INTERVALOS)
                        prom_centro.append(sum(vc)/N_INTERVALOS)
                    except Exception:
                        pass
                    
                    # Gráfico apilado en pantalla
                    if fechas and prom_norte and prom_centro and len(fechas)==len(prom_norte)==len(prom_centro):
                        fig, ax = plt.subplots(figsize=(9, 5))
                        bars_n = ax.bar(fechas, prom_norte, label="Norte")
                        bars_c = ax.bar(fechas, prom_centro, bottom=prom_norte, label="Centro")
    
                        for xlbl, a, b in zip(fechas, prom_norte, prom_centro):
                            ax.text(xlbl, a+b+1, f"{a+b:.0f}", ha="center", va="bottom", fontsize=9)
    
                        ax.set_ylabel("MW")
                        ax.set_title("HISTÓRICO EÓLICO (Potencia Promedio Diario) - Norte/Centro")
                        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                        ax.grid(axis="y", linestyle="--", alpha=0.4)
                        ax.legend()
                        plt.tight_layout()
                        eolica_figs.append(fig)
                except Exception:
                    pass
    
                # Máximo Σ/2
                fechas_lbl=[]; maximos=[]
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_eol_7:
                        vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_eol_7[l][:48]]
                        maximos.append(sum(vals)/2.0); fechas_lbl.append(l)
                    cur += timedelta(days=1)
                if maximos:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars = ax.bar(fechas_lbl, maximos)
                    for rect, val in zip(bars, maximos):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MWh")
                    ax.set_title("HISTÓRICO EÓLICO (Energía Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); eolica_figs.append(fig)
        except Exception:
            pass
    
        if eolica_figs:
            cols = st.columns(len(eolica_figs))
            for i, fig in enumerate(eolica_figs):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
    
        # =========================================================
        # ======================== SOLAR ==========================
        # =========================================================
        st.markdown("### SOLAR")
        solar_figs = []
        try:
            # HISTÓRICO IEOD
            def _extrae_solar_48_s(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_solar = None
                for c in df.columns:
                    if isinstance(c, str) and "SOLAR" in c.upper():
                        col_solar = c; break
                if not col_solar: return None
                vals = pd.to_numeric(df[col_solar].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
    
            series_ieod_solar={}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_solar_48_s(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_solar[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
    
            # HISTÓRICO RPO A
            series_sol_dia={}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                df_sol = cargar_dataframe(resultados, stem_rer)
                vals   = rellenar_hasta_48(totales_rer(df_sol, [x.upper() for x in barras_solar]))
                if vals and any(v != 0 for v in vals):
                    series_sol_dia[f.strftime("%Y-%m-%d")] = vals
    
            # Fusión + Promedio + Máximo
            series_solar_7={}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_solar:
                    series_solar_7[lbl] = series_ieod_solar[lbl][:48]
                else:
                    try:
                        fb   = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_solar_48_s(fb)
                        if vals: series_solar_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_sol_dia: series_solar_7[lbl_fin] = series_sol_dia[lbl_fin][:48]
            if series_solar_7:
                fechas_orden=[]; cur=ini
                while cur<=fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_solar_7: fechas_orden.append(l)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(12, 6)); xs=list(range(48)); y_all=[]
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_solar_7[l][:48]]
                    y_all.extend(vals); ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO SOLAR"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout()
                plt.close(fig)
                
                # Promedio
                fechas_lbl=[]; promedios=[]
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_solar_7:
                        vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_solar_7[l][:48]]
                        promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                    cur += timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars = ax.bar(fechas_lbl, promedios)
                    for rect, val in zip(bars, promedios):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                                ha="center", va="bottom", fontsize=9, clip_on=True)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW")
                    ax.set_title("HISTÓRICO SOLAR (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); solar_figs.append(fig)
    
                # Máximo Σ/2
                fechas_lbl=[]; maximos=[]
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_solar_7:
                        vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_solar_7[l][:48]]
                        maximos.append(sum(vals)/2.0); fechas_lbl.append(l)
                    cur += timedelta(days=1)
                if maximos:
                    fig, ax = plt.subplots(figsize=(9,5))
                    bars = ax.bar(fechas_lbl, maximos)
                    for rect, val in zip(bars, maximos):
                        ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                                ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MWh")
                    ax.set_title("HISTÓRICO SOLAR (Energía Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); solar_figs.append(fig)
        except Exception:
            pass
    
        if solar_figs:
            cols = st.columns(len(solar_figs))
            for i, fig in enumerate(solar_figs):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
        
    with tab7:
        # =========================================================
        # ======================= TERMICAS ========================
        # =========================================================
        st.markdown("### TÉRMICAS")
        
        def generar_grafico_termico_plotly(titulo, grupos,
                                   pdo_res, rdo_letras, work_dir,
                                   fecha_str, ddmm,
                                   stem_term="Termica - Despacho (MW)"):

            series = {}
        
            # --- PDO ---
            df_pdo = cargar_dataframe(pdo_res, stem_term)
            vals = rellenar_hasta_48(totales_rer(df_pdo, grupos))
            if vals:
                series["PDO"] = vals
        
            # --- RDO A-E ---
            for letra in rdo_letras:
                rdo_path = (
                    work_dir /
                    f"RDO_{letra}_{fecha_str}" /
                    f"YUPANA_{ddmm}{letra}" /
                    "RESULTADOS"
                )
                df_rdo = cargar_dataframe(rdo_path, stem_term)
                vals = rellenar_hasta_48(totales_rer(df_rdo, grupos))
                if vals:
                    series[f"RDO {letra}"] = vals
        
            if not series:
                st.warning(f"No hay datos para {titulo}")
                return
        
            # --------- Gráfico Plotly ---------
            fig = go.Figure()
            xs = list(range(48))
            y_all = []
        
            for name, values in series.items():
                y = []
        
                # --- convertir 0 → None (para cortar la línea) ---
                for v in values:
                    if v is None or (isinstance(v, float) and math.isnan(v)):
                        y.append(None)
                    elif v == 0:
                        y.append(None)     # NO dibujar ceros
                    else:
                        y.append(v)
        
                # si toda la curva es None, no la graficamos
                if all(v is None for v in y):
                    continue
        
                y_all.extend([v for v in y if v is not None])
        
                fig.add_trace(go.Scatter(
                    x=xs,
                    y=y,
                    mode='lines+markers',
                    name=name,
                    line=dict(width=2)
                ))
        
            if not y_all:
                st.warning(f"{titulo}: todos los valores fueron cero")
                return
            
            y_min = max(0, math.floor(min(y_all)) - 10)
            y_max = math.ceil(max(y_all)) + 10
            fig.update_yaxes(range=[y_min, y_max])
        
            fig.update_layout(
                title_text=titulo,
                # xaxis_title="Hora",
                yaxis_title="MW",
                xaxis=dict(
                    tickmode='array',
                    tickvals=ticks_pos,
                    ticktext=ticks_lbl,
                    tickangle=0
                ),
                hovermode="x unified",
            )
        
            st.plotly_chart(fig, use_container_width=True)
            
        # ===============================
        #   LISTA DE GRÁFICOS TÉRMICOS
        # ===============================
        graficos_termicos = [
            ("CHILCA 1 (Enersur/Engie)", [
                "CHILCA1TG1GAS","CHILCA1TG2GAS","CHILCA1TG3GAS",
                "CHILCA1CC1GAS","CHILCA1CC2GAS","CHILCA1CC3GAS",
                "CHILCA1CC12GAS","CHILCA1CC23GAS",
                "CHILCA1CC13GAS","CHILCA1CC123GAS",
                "CHILCA1CC13GAS", "CHILCA1CC123GAS"
            ]),
            ("CHILCA 2 (Enersur/Engie)", [
                "CHILCA2 CCOMB TG41 GAS","CHILCA2 CCOMB TG41  GAS",
                "CHILCA2 TG41  GAS"
            ]),
            ("KALLPA (KALLPA GENERACIÓN)", [
                "KALLPATG1GAS","KALLPATG2GAS","KALLPATG3GAS",
                "KALLPACC1GAS","KALLPACC2GAS","KALLPACC3GAS",
                "KALLPACC12GAS","KALLPACC23GAS","KALLPACC13GAS","KALLPACC123GAS"
            ]),
            ("FENIX (Fenix Power Perú)", [
                "FENIXGT12GAS","FENIXCCGT12GAS",
                "FENIXGT11GAS","FENIXCCGT11GAS",
                "FENIXCCGT11GT12GAS"
            ]),
            ("VENTANILLA (Orazul / Enel)", [
                "VENT3GAS","VENT4GAS",
                "VENTCC3GAS","VENTCC4GAS","VENTCC34GAS",
                "VENTCC3GASFD","VENTCC4GASFD","VENTCC34GASFD"
            ]),
            ("OLLEROS (Orazul Energy)", [
                "OLLEROSTG1GAS","OLLEROS CCOMB TG1  GAS",
                "OLLEROS CCOMB TG1 GAS"
            ]),
            ("LAS FLORES (ENGIE)", [
                "LFLORESTG1GAS","LFLORES CCOMB TG1  GAS"
            ]),
            ("INDEPENDENCIA (Termochilca)", ["INDEPGAS"]),
            # ("UTI 5", ["STA ROSA UTI 5  D2","STA ROSA UTI 5  GAS"]),
            # ("UTI 6", ["STA ROSA UTI 6  GAS","STA ROSA UTI 6  D2"]),
            ("STA ROSA (Enel)", [
                "STA ROSA WEST TG7  GAS CON H2O",
                "STA ROSA WEST TG7  GAS",
                "STAROSA TG8 GAS",
                "STA ROSA UTI 6  GAS","STA ROSA UTI 5  GAS"
            ]),
            ("MALACAS", ["MAL1TG6GAS","MALACAS3 TG 5  GAS"])
        ]
        
        # Ejecutar gráficos térmicos
        for titulo, grupos in graficos_termicos:
            generar_grafico_termico_plotly(
            titulo, grupos,
            pdo_res, rdo_letras, work_dir,
            fecha_str, ddmm
        )
        
# -----------------------------------------------------------------------------
# ------------------------------------ PDF ------------------------------------
# -----------------------------------------------------------------------------        
def render_graficos_a_pdf(ini: date, fin: date, barras: list[str], rdo_letras: list[str], work_dir: Path, pdf: PdfPages):
    return    
st.set_page_config(page_title="Reporte Programa Diario de Operación", layout="wide")
st.sidebar.header("Parámetros")
ini = st.sidebar.date_input("Inicio del rango", value=date.today(), format="DD/MM/YYYY")
fecha_sel = st.sidebar.date_input("Fecha del reporte", value=ini, format="DD/MM/YYYY")
barras = BARRAS_DEF
rdo_letras = RDO_LETRAS_DEF
fin = fecha_sel
work_dir_str = st.sidebar.text_input("Carpeta de trabajo", value=str(Path.home() / "Descargas_T"))
work_dir = Path(work_dir_str); work_dir.mkdir(parents=True, exist_ok=True)
gen_generar = st.sidebar.button("Generar", type="primary")

st.title("Reporte Programa Diario de Operación")
y, m, d = fecha_sel.year, f"{fecha_sel.month:02d}", f"{fecha_sel.day:02d}"
M = MES_TXT[int(m) - 1]

ahora_pe = datetime.now(ZoneInfo("America/Lima"))
fecha_hum = ahora_pe.strftime("%d/%m/%Y")
now_str   = ahora_pe.strftime("%H:%M")
fecha_str = f"{y}{m}{d}"
ddmm = f"{d}{m}"

st.subheader(f"Reporte del {fecha_hum}")
st.caption(f"Actualizado a las {now_str} horas")

btn_cols = st.columns([1, 8])

if gen_generar:
    with st.spinner("Descargando MOTIVOS RDO…"):
        df_motivos_local = recolectar_motivos_dia(y=y, m=m, d=d, M=M, destino=work_dir, letras="".join(rdo_letras))
        st.session_state["df_motivos"] = df_motivos_local

    with st.spinner("Descargando PDO/RDO para CMG…"):
        asegurar_insumos_para_cmg(y=y, m=m, d=d, M=M, work_dir=work_dir, rdo_letras=rdo_letras)

    with st.spinner("Renderizando en pantalla…"):
        render_graficos_en_pantalla(ini=ini, fin=fin, barras=barras, rdo_letras=rdo_letras, work_dir=work_dir)

    pdf_path = work_dir / "Reporte.pdf"
    with st.spinner("Generando PDF…"):
        pdf = PdfPages(pdf_path)
        
        # Portada
        fig, ax = plt.subplots(figsize=(11, 6)); ax.axis("off")
        ax.text(0.5, 0.7, "Reporte Programa Diario de Operación", ha="center", va="center", fontsize=20)
        ax.text(0.5, 0.5, f"Fecha: {fecha_hum}", ha="center", va="center", fontsize=14)
        ax.text(0.5, 0.4, f"Generado: {ahora_pe.strftime('%d/%m/%Y %H:%M')} (hora Perú)", ha="center", va="center", fontsize=12)
        pdf.savefig(fig); plt.close(fig)

        # =========================================================
        # =================== MOTIVOS (PDF) =======================
        # =========================================================
        df_motivos_local = st.session_state.get("df_motivos")
        if df_motivos_local is not None and not df_motivos_local.empty:
            df_motivos_local = df_motivos_local.copy().fillna("")  # evita 'nan' visibles
            n = len(df_motivos_local)
            fig_h = max(1.8, min(12, 1.0 + 0.6 * n))  # alto entre 1.8 y 12 aprox
            fig, ax = plt.subplots(figsize=(11, fig_h))
            ax.axis("off")
        
            # Asegura el orden/anchos de columnas si hace falta
            col_labels = list(df_motivos_local.columns)
            col_widths = [0.14, 0.12, 0.12, 0.62]  # suma ~1.0
        
            tabla = ax.table(
                cellText=df_motivos_local.values,
                colLabels=col_labels,
                loc="center",
                cellLoc="left",
                colWidths=col_widths
            )
        
            # Estilo encabezado
            for j in range(len(col_labels)):
                tabla[(0, j)].set_facecolor("#f0f0f0")
                tabla[(0, j)].get_text().set_weight("bold")
                tabla[(0, j)].get_text().set_ha("center")
        
            # Alineación por columna (0..3)
            for (r, c), cell in tabla.get_celld().items():
                if r == 0:
                    continue  # encabezado ya formateado
                if c in (0, 1, 2):
                    cell.get_text().set_ha("center")
                else:
                    cell.get_text().set_ha("left")
        
            tabla.auto_set_font_size(False)
            tabla.set_fontsize(8)
            tabla.scale(1, 1.2)
        
            ax.set_title("Motivo de Reprograma Diario", pad=12, fontsize=11)
            plt.tight_layout()
            pdf.savefig(fig)
            plt.close(fig)
            
        # =========================================================
        # ================= COSTO TOTAL (PDF) =====================
        # =========================================================
        resultados_por_fuente = []
        fecha_str_local = f"{y}{m}{d}" 
        pdo_root = work_dir / f"PDO_{fecha_str_local}"
        sub_pdo, filas_pdo, df_pdo_full, path_pdo = procesar_raiz(pdo_root)
        if path_pdo is None:
            df_pdo_full = None
            filas_pdo = 0
        
        if path_pdo is not None:
            resultados_por_fuente.append({
                "nombre": "PDO",
                "subtotal": sub_pdo,
                "filas": filas_pdo,
                "ruta": path_pdo,
                "df": df_pdo_full,
            })
        
        # RDOs A–E
        for letra_rdo in rdo_letras:
            rdo_root = work_dir / f"RDO_{letra_rdo}_{fecha_str_local}"
            sub_rdo, filas_rdo, df_rdo_full, path_rdo = procesar_raiz(rdo_root)
            if path_rdo is not None:
                resultados_por_fuente.append({
                    "nombre": f"RDO {letra_rdo}",
                    "subtotal": sub_rdo,
                    "filas": filas_rdo,
                    "ruta": path_rdo,
                    "df": df_rdo_full,
                })
        
        # ==================== 2) Armar datos ====================
        etiquetas = []
        valores = []
        diffs_por_barra = []
        
        for item in resultados_por_fuente:
            nombre   = item["nombre"]
            subtotal = item["subtotal"]
            filas    = item["filas"]
        
            diff_filas = filas_pdo - filas
        
            if nombre == "PDO":
                etiquetas.append(nombre)
                valores.append(subtotal)
            else:
                if (diff_filas > 0) and (df_pdo_full is not None):
                    df_pdo_head = df_pdo_full.head(diff_filas)
                    extra_subtotal, _ = sumar_costo_termica_dataframe(df_pdo_head)
                    nuevo_total = extra_subtotal + subtotal
                    etiquetas.append(nombre)
                    valores.append(nuevo_total)
                    diffs_por_barra.append(diff_filas)
                else:
                    etiquetas.append(nombre)
                    valores.append(subtotal)
                    diffs_por_barra.append(diff_filas)
        
        # ==================== 3) Gráfico PDF ====================
        if valores:
            fig, ax = plt.subplots(figsize=(11, 5))
            ax.bar(etiquetas, valores, color="#4C72B0")
        
            ymax = max(valores) if valores else 0
            ax.set_ylim(0, ymax * 1.15 if ymax > 0 else 1)
            ax.set_ylabel("$"); ax.set_title("Costo Total del PDO y RDOs")
        
            try:
                ax.yaxis.set_major_formatter(mticker.StrMethodFormatter("{x:,.0f}"))
            except Exception:
                pass
        
            for x_, v in zip(etiquetas, valores):
                ax.annotate(
                    f"{v:,.0f}",
                    xy=(x_, v),
                    xytext=(0, 3),
                    textcoords="offset points",
                    ha="center",
                    va="bottom",
                    fontsize=8,
                )
                
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        # =========================================================
        # =================== INDICES (PDF) =======================
        # =========================================================
        res_idx = extraer_listas_alfa_beta_gamma_ultimo(y, m, d, M, work_dir)
        if res_idx.get("reprograma"):
            xlbls = _build_halfhour_labels()
            alfa  = _pad_or_trim_48(res_idx.get("alfa"))
            beta  = _pad_or_trim_48(res_idx.get("beta"))
            gamma = _pad_or_trim_48(res_idx.get("gamma"))
            
            _marcas_x = []
            _marcas_lbl = []
            if 'diffs_por_barra' in locals():
                # filtra PDO (=0) 
                _marcas_x = [x for x in diffs_por_barra if x != 0]
                # etiquetas reales de RDO según tus barras (más robusto que A,B,C por índice)
                if 'etiquetas' in locals():
                    _marcas_lbl = [e for e in etiquetas if e.startswith("RDO")]
                else:
                    _marcas_lbl = [f"RDO {chr(65+i)}" for i in range(len(_marcas_x))]
        
            _plot_indices_pdf(xlbls, alfa,  "ALFA (HIDRO)",  pdf, marcas_x=_marcas_x, marcas_lbl=_marcas_lbl)
            _plot_indices_pdf(xlbls, beta,  "BETA (TERMO)",  pdf, marcas_x=_marcas_x, marcas_lbl=_marcas_lbl)
            _plot_indices_pdf(xlbls, gamma, "GAMMA (DEMANDA)", pdf, marcas_x=_marcas_x, marcas_lbl=_marcas_lbl)
            
        # CMG (PDF)
        horas, ticks_pos, ticks_lbl = _build_time_labels_and_ticks()
        stem_file = "CMg - Barra ($ por MWh)"
        pdo_res = work_dir / f"PDO_{fecha_str}" / f"YUPANA_{fecha_str}" / "RESULTADOS"
        for barra in barras:
            df_pdo   = cargar_dataframe(pdo_res, stem_file)
            datosPDO = rellenar_hasta_48(extraer_columna(df_pdo, barra))
            if not datosPDO: continue
            series_barra = {"PDO": datosPDO}
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                df_rdo   = cargar_dataframe(rdo_res, stem_file)
                datosRDO = rellenar_hasta_48(extraer_columna(df_rdo, barra))
                if datosRDO: series_barra[f"RDO {letra}"] = datosRDO
            fig, ax = plt.subplots(figsize=(11, 5))
            ok = _plot_cmg_barra_en_axes(ax, barra, series_barra, horas, ticks_pos, ticks_lbl)
            if ok: plt.tight_layout(); pdf.savefig(fig)
            plt.close(fig)
        
        # =========================================================
        # =============== DEMANDA Y ERROR (PDF) ===================
        # =========================================================
        try:
            archivos_dem = {
                "HIDRO"   : "Hidro - Despacho (MW)",
                "TERMICA" : "Termica - Despacho (MW)",
                "RER"     : "Rer y No COES - Despacho (MW)"
            }
        
            # ---------- DEMANDA (PDO + RDOs) ----------
            series_dem = {}
        
            vals_hidro_p   = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos_dem["HIDRO"])))
            vals_termica_p = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos_dem["TERMICA"])))
            vals_rer_p     = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(pdo_res, archivos_dem["RER"])))
            series_dem["PDO"] = suma_elementos(vals_hidro_p, vals_termica_p, vals_rer_p)
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos_dem["HIDRO"])))
                vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos_dem["TERMICA"])))
                vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(rdo_res, archivos_dem["RER"])))
                if any((vals_h, vals_t, vals_r)):
                    series_dem[f"RDO {letra}"] = suma_elementos(vals_h, vals_t, vals_r)
        
            if series_dem:
                # --- Gráfico DEMANDA ---
                fig, ax = plt.subplots(figsize=(11, 5))
                yvals = []
                for nombre, valores in series_dem.items():
                    xlab, yv = recortar_ceros_inicio(valores, horas)
                    if not yv:
                        continue
                    start = len(horas) - len(yv)
                    xnum = np.arange(start, start + len(yv))
                    yvals.extend(yv)
                    ax.plot(xnum, yv, marker="o", linewidth=2, label=nombre)
        
                aplicar_formato_xy(
                    ax, L=len(horas),
                    ticks_pos=ticks_pos, horas=horas,
                    y_values=yvals, ypad=0.05, xpad=0.5
                )
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("DEMANDA")
                ax.set_ylabel("MW")
                ax.legend()
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
        
                # ---------- ERRORES relativos absolutos ----------
                # Pares consecutivos: ("PDO","RDO A"), ("RDO A","RDO B"), ...
                orden_series_dem = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_dem]
                pares_dem = [(orden_series_dem[i], orden_series_dem[i+1]) for i in range(len(orden_series_dem)-1)]
        
                curvas = []   # cada item: {"label":..., "y_clean": np.array(...), "start_idx": int}
                L_global = None
        
                for (ante, act) in pares_dem:
                    va = series_dem[ante]
                    vb = series_dem[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
        
                    etiqueta = f"Error {ante} - {act}"
                    vals = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
                    y_clean = np.array([_omit_0_100(v) for v in vals], dtype=float)
        
                    if L_global is None:
                        L_global = mL
                    else:
                        L_global = min(L_global, mL)
        
                    curvas.append({"label": etiqueta, "y_clean": y_clean})
        
                if curvas:
                    # Longitud común
                    L = L_global
                    x = np.arange(L)
        
                    # Detectar inicio válido de cada curva
                    for c in curvas:
                        ysub = c["y_clean"][:L]
                        not_nan = np.where(~np.isnan(ysub))[0]
                        if len(not_nan) == 0:
                            c["start_idx"] = None
                        else:
                            c["start_idx"] = int(not_nan[0])
        
                    # Arreglo final y cortes: "cuando empieza la nueva, la anterior se apaga"
                    y_final_list = [c["y_clean"][:L].copy() for c in curvas]
                    for i in range(1, len(curvas)):
                        s_new = curvas[i]["start_idx"]
                        if s_new is None:
                            continue
                        for j in range(i):
                            y_final_list[j][s_new:] = np.nan
        
                    # --- Gráfico ERROR DEMANDA ---
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata_all = []
                    for i, c in enumerate(curvas):
                        serie_plot = y_final_list[i]
                        ydata_all.extend(serie_plot[~np.isnan(serie_plot)])
                        ax.plot(x, serie_plot, marker='o', linewidth=2, label=c["label"])
        
                    aplicar_formato_xy(
                        ax, L=L,
                        ticks_pos=ticks_pos, horas=horas,
                        y_values=ydata_all, ypad=0.05, xpad=0.5
                    )
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de DEMANDA")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)
        
        except Exception:
            pass
        
        # =========================================================
        # =============== HIDRO Y ERROR (PDF) =====================
        # =========================================================
        try:
            barras_rer = [
                "CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"
            ]
            stem_hidro = "Hidro - Despacho (MW)"
            stem_rer   = "Rer y No COES - Despacho (MW)"
        
            series_h = {}
            df_pdo_h   = cargar_dataframe(pdo_res, stem_hidro)
            df_pdo_rer = cargar_dataframe(pdo_res, stem_rer)
            tot_hidro = rellenar_hasta_48(totales_hidro(df_pdo_h))
            tot_rer   = rellenar_hasta_48(totales_rer(df_pdo_rer, [x.upper() for x in barras_rer]))
            if tot_hidro and tot_rer:
                series_h["PDO"] = suma_elementos(tot_hidro, tot_rer)
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(rdo_res, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer(cargar_dataframe(rdo_res, stem_rer), [x.upper() for x in barras_rer]))
                if th and tr:
                    series_h[f"RDO {letra}"] = suma_elementos(th, tr)
        
            if series_h:
                fig, ax = plt.subplots(figsize=(11, 5))
                yvals_h = []
                for nombre, valores in series_h.items():
                    xlab, yv = recortar_ceros_inicio(valores, horas)
                    if not yv:
                        continue
                    start = len(horas) - len(yv)
                    xnum = np.arange(start, start + len(yv))
                    yvals_h.extend(yv)
                    ax.plot(xnum, yv, marker="o", linewidth=2, label=nombre)
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas, y_values=yvals_h, ypad=0.05, xpad=0.5)
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HIDRO")
                ax.set_ylabel("MW")
                ax.legend()
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
        
                # --- Error HIDRO ---
                orden_series = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_h]
                pares = [(orden_series[i], orden_series[i+1]) for i in range(len(orden_series)-1)]
                curvas = []
                L_global = None
                for (ante, act) in pares:
                    va, vb = series_h[ante], series_h[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
                    etiqueta = f"Error {ante} - {act}"
                    vals = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
                    y_clean = np.array([_omit_0_100(v) for v in vals], dtype=float)
                    if L_global is None:
                        L_global = mL
                    else:
                        L_global = min(L_global, mL)
                    curvas.append({"label": etiqueta, "y_clean": y_clean})
        
                if curvas:
                    L = L_global
                    x = np.arange(L)
                    for c in curvas:
                        ysub = c["y_clean"][:L]
                        not_nan = np.where(~np.isnan(ysub))[0]
                        c["start_idx"] = int(not_nan[0]) if len(not_nan) else None
                    y_final_list = [c["y_clean"][:L].copy() for c in curvas]
                    for i in range(1, len(curvas)):
                        s_new = curvas[i]["start_idx"]
                        if s_new is None:
                            continue
                        for j in range(i):
                            y_final_list[j][s_new:] = np.nan
        
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata_e = []
                    for i, c in enumerate(curvas):
                        serie_plot = y_final_list[i]
                        ydata_e.extend(serie_plot[~np.isnan(serie_plot)])
                        ax.plot(x, serie_plot, marker='o', linewidth=2, label=c["label"])
                    aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas, y_values=ydata_e, ypad=0.05, xpad=0.5)
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de HIDRO")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)
        except Exception:
            pass
        
        # =========================================================
        # =============== EÓLICA Y ERROR (PDF) ====================
        # =========================================================
        try:
            stem_rer = "Rer y No COES - Despacho (MW)"
            barras_eol = [
                "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
                "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
                "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"
            ]
        
            series_rer = {}
            df_pdo_rer = cargar_dataframe(pdo_res, stem_rer)
            vals_pdo = rellenar_hasta_48(totales_rer(df_pdo_rer, [x.upper() for x in barras_eol]))
            if vals_pdo:
                series_rer["PDO"] = vals_pdo
        
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                df_rdo = cargar_dataframe(rdo_res, stem_rer)
                vals_rdo = rellenar_hasta_48(totales_rer(df_rdo, [x.upper() for x in barras_eol]))
                if vals_rdo:
                    series_rer[f"RDO {letra}"] = vals_rdo
        
            if series_rer:
                fig, ax = plt.subplots(figsize=(11, 5))
                y_plot = []
                for nombre, valores in series_rer.items():
                    xlab, yv = recortar_ceros_inicio(valores, horas)
                    if not yv:
                        continue
                    start = len(horas) - len(yv)
                    xnum = np.arange(start, start + len(yv))
                    y_plot.extend(yv)
                    ax.plot(xnum, yv, marker="o", linewidth=2, label=nombre)
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas, y_values=y_plot, ypad=0.05, xpad=0.5)
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("EÓLICO")
                ax.set_ylabel("MW")
                ax.legend()
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
        
                # Error EÓLICO
                orden_series_eol = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_rer]
                pares_eol = [(orden_series_eol[i], orden_series_eol[i+1]) for i in range(len(orden_series_eol)-1)]
                curvas = []
                L_global = None
                for (ante, act) in pares_eol:
                    va, vb = series_rer[ante], series_rer[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
                    etiqueta = f"Error {ante} - {act}"
                    vals = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
                    y_clean = np.array([_omit_0_100(v) for v in vals], dtype=float)
                    if L_global is None:
                        L_global = mL
                    else:
                        L_global = min(L_global, mL)
                    curvas.append({"label": etiqueta, "y_clean": y_clean})
                if curvas:
                    L = L_global
                    x = np.arange(L)
                    for c in curvas:
                        ysub = c["y_clean"][:L]
                        not_nan = np.where(~np.isnan(ysub))[0]
                        c["start_idx"] = int(not_nan[0]) if len(not_nan) else None
                    y_final_list = [c["y_clean"][:L].copy() for c in curvas]
                    for i in range(1, len(curvas)):
                        s_new = curvas[i]["start_idx"]
                        if s_new is None:
                            continue
                        for j in range(i):
                            y_final_list[j][s_new:] = np.nan
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata = []
                    for i, c in enumerate(curvas):
                        serie_plot = y_final_list[i]
                        ydata.extend(serie_plot[~np.isnan(serie_plot)])
                        ax.plot(x, serie_plot, marker='o', linewidth=2, label=c["label"])
                    aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas, y_values=ydata, ypad=0.05, xpad=0.5)
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de EÓLICO")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)
        except Exception:
            pass
        
        # =========================================================
        # =============== SOLAR Y ERROR (PDF) =====================
        # =========================================================
        try:
            stem_rer = "Rer y No COES - Despacho (MW)"
            barras_solar = [
                "MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
                "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
                "CS MATARANI","CS SAN MARTIN","CS SUNNY"
            ]
            series_sol = {}
            df_pdo_sol = cargar_dataframe(pdo_res, stem_rer)
            vals_pdo = rellenar_hasta_48(totales_rer(df_pdo_sol, [x.upper() for x in barras_solar]))
            if vals_pdo:
                series_sol["PDO"] = vals_pdo
            for letra in rdo_letras:
                rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                df_rdo_sol = cargar_dataframe(rdo_res, stem_rer)
                vals_rdo = rellenar_hasta_48(totales_rer(df_rdo_sol, [x.upper() for x in barras_solar]))
                if vals_rdo and any(v != 0 for v in vals_rdo):
                    series_sol[f"RDO {letra}"] = vals_rdo
            if series_sol:
                fig, ax = plt.subplots(figsize=(11, 5))
                y_plot = []
                for nombre, raw_vals in series_sol.items():
                    y_vals = []
                    for i, v in enumerate(raw_vals[:48]):
                        v = 0 if pd.isna(v) else v
                        if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                            y_vals.append(None)
                        else:
                            y_vals.append(v)
                    if all(v is None for v in y_vals):
                        continue
                    xnum = np.arange(len(horas))
                    y_plot.extend([v for v in y_vals if v is not None])
                    ax.plot(xnum, y_vals, marker="o", linewidth=2, label=nombre)
                aplicar_formato_xy(ax, L=len(horas), ticks_pos=ticks_pos, horas=horas, y_values=y_plot, ypad=0.05, xpad=0.5)
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("SOLAR")
                ax.set_ylabel("MW")
                ax.legend()
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)
        
                # Error Solar
                orden_series_sol = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_sol]
                pares_sol = [(orden_series_sol[i], orden_series_sol[i+1]) for i in range(len(orden_series_sol)-1)]
                errores_sol = {}
                for (ante, act) in pares_sol:
                    va, vb = series_sol[ante], series_sol[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
                    etiqueta = f"Error {ante} - {act}"
                    errores_sol[etiqueta] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
                if errores_sol:
                    L = min(len(v) for v in errores_sol.values() if v) or len(horas)
                    x = np.arange(L)
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata = []
                    for (ante, act) in pares_sol:
                        etiqueta = f"Error {ante} - {act}"
                        if etiqueta not in errores_sol:
                            continue
                        serie_vals = errores_sol[etiqueta]
                        serie = np.array([_omit_0_100(v) for v in serie_vals[:L]], dtype=float)
                        ydata.extend(serie[~np.isnan(serie)])
                        ax.plot(x, serie, marker='o', linewidth=2, label=etiqueta)
                    aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas, y_values=ydata, ypad=0.05, xpad=0.5)
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de SOLAR")
                    ax.set_ylabel("%")
                    ax.legend()
                    plt.tight_layout()
                    pdf.savefig(fig)
                    plt.close(fig)
        except Exception:
            pass
        
        # =========================================================
        # ============= HISTÓRICO HIDRO (PDF) =====================
        # =========================================================
        try:
            hidro_pdf_figs = []
        
            # --- IEOD HIDRO (opcional: deja armado por si quieres reactivar) ---
            series_por_dia = []
            dias = (fin - ini).days + 1
            for k in range(dias):
                f = ini + timedelta(days=k)
                try:
                    fb = _lee_ieod_bytes(f.year, f.month, MES_TXT[f.month-1], f.day)
                    df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                    c_pas, c_reg = _find_cols_ieod(df)
                    if not c_pas or not c_reg:
                        continue
                    sub = df.iloc[0:48, :]
                    pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                    reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                    v_sum = [pas[i] + reg[i] for i in range(48)]
                    series_por_dia.append((f.strftime("%Y-%m-%d"), v_sum))
                except Exception:
                    pass
                
            # --- RDO A – histórico ---
            series_dia = {}
            stem_hidro = "Hidro - Despacho (MW)"
            stem_rer   = "Rer y No COES - Despacho (MW)"
            barras_rer_up = ["CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                             "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                             "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                             "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"]
            dias = (fin - ini).days + 1
            for k in range(dias):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
                if th and tr:
                    series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
                    
            # --- FUSIÓN (IEOD días previos + RDO A día fin) ---
            series_7 = {}
            ini_ieod = ini
            fin_ieod = fin - timedelta(days=1)
            dias_ieod = (fin_ieod - ini_ieod).days + 1 if fin_ieod >= ini_ieod else 0
            # IEOD previos
            for k in range(dias_ieod):
                f = ini_ieod + timedelta(days=k)
                try:
                    fb = _lee_ieod_bytes(f.year, f.month, MES_TXT[f.month-1], f.day)
                    df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                    c_pas, c_reg = _find_cols_ieod(df)
                    if not c_pas or not c_reg:
                        continue
                    sub = df.iloc[0:48, :]
                    pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                    reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                    vals = [pas[i] + reg[i] for i in range(48)]
                    series_7[f.strftime("%Y-%m-%d")] = vals
                except Exception:
                    pass
            # RDO-A último día
            try:
                f_last = fin
                yk, mk, dk = f_last.year, f_last.strftime("%m"), f_last.strftime("%d"); M_TXT = MES_TXT[f_last.month-1]
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
                if th and tr:
                    series_7[f_last.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
            except Exception:
                pass
        
            if series_7:
                fechas_orden = []
                cur = ini
                while cur <= fin:
                    lbl = cur.strftime("%Y-%m-%d")
                    if lbl in series_7: fechas_orden.append(lbl)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(11, 5))
                xs = list(range(48)); y_all = []
                for lbl in fechas_orden:
                    fobj = datetime.strptime(lbl, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = series_7[lbl]
                    y_all.extend(vals)
                    ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=lbl)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO HIDRO"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        except Exception:
            pass
        
        # =========================================================
        # =========== HISTÓRICO DEMANDA (PDF) ====================
        # =========================================================
        try:
            demanda_pdf_figs = []
        
            # IEOD DEMANDA (días previos) + RDO A (día fin) -> fusión
            series_ieod_dem = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_demanda_48(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_dem[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            # RPO A por día
            series_dia = {}
            archivos_dem = {
                "HIDRO"   : "Hidro - Despacho (MW)",
                "TERMICA" : "Termica - Despacho (MW)",
                "RER"     : "Rer y No COES - Despacho (MW)"
            }
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["HIDRO"])))
                vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["TERMICA"])))
                vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["RER"])))
                if any((vals_h, vals_t, vals_r)):
                    series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(vals_h, vals_t, vals_r)
        
            # Fusión: IEOD (todos menos el último) + RDO A último día
            series_dem_7 = {}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_dem:
                    series_dem_7[lbl] = series_ieod_dem[lbl][:48]
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_dia:
                series_dem_7[lbl_fin] = series_dia[lbl_fin][:48]
        
            if series_dem_7:
                fechas_orden = []
                cur = ini
                while cur <= fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_dem_7: fechas_orden.append(l)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(11, 5)); xs = list(range(48)); y_all = []
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_dem_7[l][:48]]
                    y_all.extend(vals)
                    ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO DEMANDA"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        except Exception:
            pass

        # =========================================================
        # ============ HISTÓRICO EÓLICO (PDF) ====================
        # =========================================================
        try:
            eolica_pdf_figs = []
        
            def _extrae_eolica_48(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_eolica = None
                for c in df.columns:
                    if isinstance(c, str):
                        c_norm = _sin_acentos(c).upper().strip()
                        if "EOLICA" in c_norm:
                            col_eolica = c; break
                if not col_eolica: return None
                vals = pd.to_numeric(df[col_eolica].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
        
            # IEOD eólico
            series_ieod_eol = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_eolica_48(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_eol[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            # RDO A eólico por día
            stem_rer = "Rer y No COES - Despacho (MW)"
            barras_eol = ["PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
                          "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
                          "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"]
            series_eol_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                df_rer = cargar_dataframe(resultados, stem_rer)
                tot_eol = rellenar_hasta_48(totales_rer(df_rer, [x.upper() for x in barras_eol]))
                if tot_eol:
                    series_eol_dia[f.strftime("%Y-%m-%d")] = tot_eol
        
            # Fusión
            series_eol_7 = {}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_eol:
                    series_eol_7[lbl] = series_ieod_eol[lbl][:48]
                else:
                    try:
                        fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_eolica_48(fb)
                        if vals: series_eol_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_eol_dia:
                series_eol_7[lbl_fin] = series_eol_dia[lbl_fin][:48]
        
            if series_eol_7:
                fechas_orden=[]; cur=ini
                while cur<=fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_eol_7: fechas_orden.append(l)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(11, 5)); xs=list(range(48)); y_all=[]
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_eol_7[l][:48]]
                    y_all.extend(vals)
                    ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO EÓLICO"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        except Exception:
            pass
        
        # =========================================================
        # ============= HISTÓRICO SOLAR (PDF) =====================
        # =========================================================
        try:
            solar_pdf_figs = []
        
            def _extrae_solar_48_s(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_solar = None
                for c in df.columns:
                    if isinstance(c, str) and "SOLAR" in c.upper():
                        col_solar = c; break
                if not col_solar: return None
                vals = pd.to_numeric(df[col_solar].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
        
            # IEOD SOLAR
            series_ieod_solar = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_solar_48_s(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_solar[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            # RDO A SOLAR
            stem_rer = "Rer y No COES - Despacho (MW)"
            barras_solar = ["MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
                            "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
                            "CS MATARANI","CS SAN MARTIN","CS SUNNY"]
            series_sol_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                df_sol = cargar_dataframe(resultados, stem_rer)
                vals   = rellenar_hasta_48(totales_rer(df_sol, [x.upper() for x in barras_solar]))
                if vals and any(v != 0 for v in vals):
                    series_sol_dia[f.strftime("%Y-%m-%d")] = vals
        
            # Fusión (IEOD previos + RDO A fin)
            series_solar_7 = {}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_solar:
                    series_solar_7[lbl] = series_ieod_solar[lbl][:48]
                else:
                    try:
                        fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                        vals = _extrae_solar_48_s(fb)
                        if vals: series_solar_7[lbl] = vals[:48]
                    except Exception:
                        pass
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_sol_dia:
                series_solar_7[lbl_fin] = series_sol_dia[lbl_fin][:48]
        
            if series_solar_7:
                fechas_orden=[]; cur=ini
                while cur<=fin:
                    l = cur.strftime("%Y-%m-%d")
                    if l in series_solar_7: fechas_orden.append(l)
                    cur += timedelta(days=1)
                fig, ax = plt.subplots(figsize=(11, 5)); xs=list(range(48)); y_all=[]
                for l in fechas_orden:
                    fobj = datetime.strptime(l, "%Y-%m-%d").date()
                    estilo = '--' if fobj < fin else '-'
                    vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_solar_7[l][:48]]
                    y_all.extend(vals)
                    ax.plot(xs, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                if y_all:
                    ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_title("HISTÓRICO SOLAR"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        except Exception:
            pass
        
        # ======================== HIDRO → PDF ========================
        try:
            # --- H. PASADA vs H. REGULACIÓN (día fin) ---
            def _norm(txt):
                import re; return re.sub(r"\s+"," ",str(txt).strip()).upper()
            def _find_cols(cols):
                c_pas = c_reg = None
                for c in cols:
                    k = _norm(c)
                    if k == "H. PASADA" and c_pas is None: c_pas = c
                    if k == "H. REGULACION" and c_reg is None: c_reg = c
                return c_pas, c_reg
            def _extrae_listas_48(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                c_pas, c_reg = _find_cols(df.columns)
                if not c_pas or not c_reg: return None, None, None
                sub = df.iloc[0:48, :]
                pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()
                reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()
                pas = (pas + [0.0]*48)[:48]; reg = (reg + [0.0]*48)[:48]
                return pas, reg, [pas[i]+reg[i] for i in range(48)]
        
            f = fin; y2, m2, d2 = f.year, f.month, f.day; M2 = MES_TXT[m2-1]
            fb = _lee_ieod_bytes(y2, m2, M2, d2)
            v_pas, v_reg, v_sum = _extrae_listas_48(fb)
            if v_sum is not None:
                fig, ax = plt.subplots(figsize=(11, 5))
                xs = list(range(48))
                ax.bar(xs, v_pas, label="H. PASADA")
                ax.bar(xs, v_reg, bottom=v_pas, label="H. REGULACION")
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                ax.set_title("H. PASADA - H. REGULACIÓN"); ax.set_ylabel("MW")
                ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend()
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass
        
        # --- Histórico HIDRO (fusión IEOD días previos + RDO-A día fin) + Promedio + Energía ---
        try:
            series_7 = {}
            stem_hidro="Hidro - Despacho (MW)"; stem_rer="Rer y No COES - Despacho (MW)"
            barras_rer_up = ["CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                             "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                             "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                             "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"]
        
            # IEOD: ini → fin-1
            ini_ieod = ini; fin_ieod = fin - timedelta(days=1)
            dias_ieod = (fin_ieod - ini_ieod).days + 1 if fin_ieod >= ini_ieod else 0
            for k in range(dias_ieod):
                f = ini_ieod + timedelta(days=k); M2 = MES_TXT[f.month-1]
                try:
                    fb = _lee_ieod_bytes(f.year, f.month, M2, f.day)
                    df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                    def _find_cols_ieod(df_):
                        def _n(s): import re; return re.sub(r"\s+"," ",str(s).strip()).upper()
                        c_pas=c_reg=None
                        for c in df_.columns:
                            k=_n(c)
                            if k=="H. PASADA" and c_pas is None: c_pas=c
                            if k=="H. REGULACION" and c_reg is None: c_reg=c
                        return c_pas, c_reg
                    c_pas, c_reg = _find_cols_ieod(df)
                    if c_pas and c_reg:
                        sub = df.iloc[0:48, :]
                        pas = pd.to_numeric(sub[c_pas], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                        reg = pd.to_numeric(sub[c_reg], errors="coerce").fillna(0.0).astype(float).tolist()[:48]
                        series_7[f.strftime("%Y-%m-%d")] = [pas[i]+reg[i] for i in range(48)]
                except Exception:
                    pass
        
            # RDO-A: día fin
            try:
                f_last = fin; yk, mk, dk = f_last.year, f_last.strftime("%m"), f_last.strftime("%d"); M_TXT = MES_TXT[f_last.month-1]
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
                tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer_up]))
                if th and tr: series_7[f_last.strftime("%Y-%m-%d")] = suma_elementos(th, tr)
            except Exception:
                pass
        
            if series_7:
                # Promedio diario
                fechas_lbl=[]; promedios=[]; cur=ini
                while cur<=fin:
                    lbl=cur.strftime("%Y-%m-%d")
                    if lbl in series_7:
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_7[lbl][:48]]
                        promedios.append(sum(vals)/len(vals)); fechas_lbl.append(lbl)
                    cur+=timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars=ax.bar(fechas_lbl, promedios)
                    for r,v in zip(bars, promedios):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height()+1, f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO HIDRO (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
                # Energía diaria (Σ/2)
                fechas_lbl=[]; energia=[]
                cur=ini
                while cur<=fin:
                    lbl=cur.strftime("%Y-%m-%d")
                    if lbl in series_7:
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_7[lbl][:48]]
                        energia.append(sum(vals)/2.0); fechas_lbl.append(lbl)
                    cur+=timedelta(days=1)
                if energia:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars=ax.bar(fechas_lbl, energia)
                    for r,v in zip(bars, energia):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MWh"); ax.set_title("HISTÓRICO HIDRO (Energía Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass

        # ======================== DEMANDA → PDF ========================
        try:
            # IEOD + RDO-A fusión
            series_ieod_dem = {}
            cur = ini
            while cur <= fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_demanda_48(fb)
                    if vals and any(v != 0 for v in vals):
                        series_ieod_dem[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception:
                    pass
                cur += timedelta(days=1)
        
            archivos_dem = {
                "HIDRO":"Hidro - Despacho (MW)",
                "TERMICA":"Termica - Despacho (MW)",
                "RER":"Rer y No COES - Despacho (MW)"
            }
            series_dia = {}
            for k in range((fin - ini).days + 1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r = requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception:
                        continue
                vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["HIDRO"])))
                vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["TERMICA"])))
                vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(resultados, archivos_dem["RER"])))
                if any((vals_h, vals_t, vals_r)):
                    series_dia[f.strftime("%Y-%m-%d")] = suma_elementos(vals_h, vals_t, vals_r)
        
            series_dem_7={}
            cur = ini
            while cur < fin:
                lbl = cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_dem: series_dem_7[lbl] = series_ieod_dem[lbl][:48]
                cur += timedelta(days=1)
            lbl_fin = fin.strftime("%Y-%m-%d")
            if lbl_fin in series_dia: series_dem_7[lbl_fin] = series_dia[lbl_fin][:48]
        
            if series_dem_7:
                # Promedio
                fechas_lbl=[]; promedios=[]; cur=ini
                while cur<=fin:
                    l=cur.strftime("%Y-%m-%d")
                    if l in series_dem_7:
                        vals=[0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_dem_7[l][:48]]
                        promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                    cur+=timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars = ax.bar(fechas_lbl, promedios)
                    for r,v in zip(bars, promedios):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO DEMANDA (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
                # Máximo diario
                fechas_lbl=[]; maximos=[]; cur=ini
                while cur<=fin:
                    l=cur.strftime("%Y-%m-%d")
                    if l in series_dem_7:
                        vals=[0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_dem_7[l][:48]]
                        maximos.append(max(vals) if vals else 0.0); fechas_lbl.append(l)
                    cur+=timedelta(days=1)
                if maximos:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars = ax.bar(fechas_lbl, maximos)
                    for r,v in zip(bars, maximos):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO DEMANDA (Máxima Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass

        # ======================== EÓLICA → PDF ========================
        try:
            def _extrae_eolica_48(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_eolica=None
                for c in df.columns:
                    if isinstance(c,str) and "EOLICA" in _sin_acentos(c).upper().strip():
                        col_eolica=c; break
                if not col_eolica: return None
                vals = pd.to_numeric(df[col_eolica].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
        
            # IEOD
            series_ieod_eol={}; cur=ini
            while cur<=fin:
                try:
                    fb=_lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals=_extrae_eolica_48(fb)
                    if vals and any(v!=0 for v in vals): series_ieod_eol[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception: pass
                cur+=timedelta(days=1)
        
            # RDO-A
            stem_rer="Rer y No COES - Despacho (MW)"
            barras_eol=["PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS","WAYRAI","HUAMBOS","DUNA",
                        "CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2","PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"]
            series_eol_dia={}
            for k in range((fin-ini).days+1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r=requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception: continue
                df_rer = cargar_dataframe(resultados, stem_rer)
                tot_eol = rellenar_hasta_48(totales_rer(df_rer, [x.upper() for x in barras_eol]))
                if tot_eol: series_eol_dia[f.strftime("%Y-%m-%d")] = tot_eol
        
            # Fusión
            series_eol_7={}
            cur=ini
            while cur<fin:
                lbl=cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_eol: series_eol_7[lbl]=series_ieod_eol[lbl][:48]
                cur+=timedelta(days=1)
            lbl_fin=fin.strftime("%Y-%m-%d")
            if lbl_fin in series_eol_dia: series_eol_7[lbl_fin]=series_eol_dia[lbl_fin][:48]
        
            if series_eol_7:
                # Promedio diario (total)
                fechas_lbl=[]; promedios=[]; cur=ini
                while cur<=fin:
                    l=cur.strftime("%Y-%m-%d")
                    if l in series_eol_7:
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_eol_7[l][:48]]
                        promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                    cur+=timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars=ax.bar(fechas_lbl, promedios)
                    for r,v in zip(bars, promedios):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO EÓLICO (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
                # Promedio diario Norte/Centro (IEOD previos + último día RDO-A)
                try:
                    N_INTERVALOS=48
                    def _norm_txt(s): return unicodedata.normalize("NFKD", str(s)).encode("ASCII","ignore").decode().upper().strip()
                    def _extrae_eolica_ns_gareas(fbytes):
                        df = pd.read_excel(fbytes, sheet_name="G_AREAS", header=None, engine="openpyxl")
                        fila_rot=col_norte=col_centro=None
                        for i in df.index:
                            for j in df.columns:
                                v=_norm_txt(df.iat[i,j])
                                if "GENERACION EOLICA" in v:
                                    if "NORTE" in v: fila_rot, col_norte = i, j
                                    if "CENTRO" in v: fila_rot = i if fila_rot is None else fila_rot; col_centro = j
                            if fila_rot is not None and col_norte is not None and col_centro is not None: break
                        if fila_rot is None or col_norte is None or col_centro is None: return None, None
                        v_norte  = pd.to_numeric(df.iloc[fila_rot+1:fila_rot+1+N_INTERVALOS, col_norte ], errors="coerce").fillna(0.0).astype(float).tolist()
                        v_centro = pd.to_numeric(df.iloc[fila_rot+1:fila_rot+1+N_INTERVALOS, col_centro], errors="coerce").fillna(0.0).astype(float).tolist()
                        return (v_norte+[0.0]*N_INTERVALOS)[:N_INTERVALOS], (v_centro+[0.0]*N_INTERVALOS)[:N_INTERVALOS]
        
                    fechas, prom_norte, prom_centro = [], [], []
                    cur=ini
                    while cur<fin:
                        try:
                            fb=_lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                            vn, vc = _extrae_eolica_ns_gareas(fb)
                            if vn is not None and vc is not None:
                                fechas.append(cur.strftime("%Y-%m-%d"))
                                prom_norte.append(sum(vn)/N_INTERVALOS)
                                prom_centro.append(sum(vc)/N_INTERVALOS)
                        except Exception: pass
                        cur+=timedelta(days=1)
        
                    # último día con RDO-A dividido Norte/Centro
                    try:
                        f=fin; yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                        carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                        resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                        if not resultados.exists():
                            r=requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                            with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                        df_rer = cargar_dataframe(resultados, stem_rer)
                        NORTE={"PE TALARA","PE CUPISNIQUE","HUAMBOS","DUNA"}
                        CENTRO=set(x.upper() for x in barras_eol) - NORTE
                        vn = rellenar_hasta_48(totales_rer(df_rer, list(NORTE)))  or [0.0]*N_INTERVALOS
                        vc = rellenar_hasta_48(totales_rer(df_rer, list(CENTRO))) or [0.0]*N_INTERVALOS
                        fechas.append(f.strftime("%Y-%m-%d"))
                        prom_norte.append(sum(vn)/N_INTERVALOS)
                        prom_centro.append(sum(vc)/N_INTERVALOS)
                    except Exception:
                        pass
        
                    if fechas and len(fechas)==len(prom_norte)==len(prom_centro):
                        fig, ax = plt.subplots(figsize=(11,5))
                        bars_n = ax.bar(fechas, prom_norte, label="Norte")
                        bars_c = ax.bar(fechas, prom_centro, bottom=prom_norte, label="Centro")
                        for xlbl, a, b in zip(fechas, prom_norte, prom_centro):
                            ax.text(xlbl, a+b+1, f"{a+b:.0f}", ha="center", va="bottom", fontsize=9)
                        ax.set_ylabel("MW"); ax.set_title("HISTÓRICO EÓLICO (Promedio Diario) - Norte/Centro")
                        ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                        ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend()
                        plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
                except Exception:
                    pass
        
                # Energía diaria (Σ/2)
                fechas_lbl=[]; energia=[]; cur=ini
                while cur<=fin:
                    l=cur.strftime("%Y-%m-%d")
                    if l in series_eol_7:
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_eol_7[l][:48]]
                        energia.append(sum(vals)/2.0); fechas_lbl.append(l)
                    cur+=timedelta(days=1)
                if energia:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars=ax.bar(fechas_lbl, energia)
                    for r,v in zip(bars, energia):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MWh"); ax.set_title("HISTÓRICO EÓLICO (Energía Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass
        
        # ======================== SOLAR → PDF ========================
        try:
            def _extrae_solar_48_s(fbytes):
                df = pd.read_excel(fbytes, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
                col_solar=None
                for c in df.columns:
                    if isinstance(c,str) and "SOLAR" in c.upper(): col_solar=c; break
                if not col_solar: return None
                vals = pd.to_numeric(df[col_solar].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
                return (vals + [0.0]*48)[:48]
        
            # IEOD
            series_ieod_solar={}; cur=ini
            while cur<=fin:
                try:
                    fb=_lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals=_extrae_solar_48_s(fb)
                    if vals and any(v!=0 for v in vals): series_ieod_solar[cur.strftime("%Y-%m-%d")] = vals[:48]
                except Exception: pass
                cur+=timedelta(days=1)
        
            # RDO-A
            stem_rer="Rer y No COES - Despacho (MW)"
            barras_solar=["MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
                          "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
                          "CS MATARANI","CS SAN MARTIN","CS SUNNY"]
            series_sol_dia={}
            for k in range((fin-ini).days+1):
                f = ini + timedelta(days=k)
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d"); M_TXT = MES_TXT[f.month-1]
                url_zip = base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A")
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    try:
                        r=requests.get(url_zip, timeout=40); r.raise_for_status()
                        with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                    except Exception: continue
                df_sol = cargar_dataframe(resultados, stem_rer)
                vals   = rellenar_hasta_48(totales_rer(df_sol, [x.upper() for x in barras_solar]))
                if vals and any(v!=0 for v in vals): series_sol_dia[f.strftime("%Y-%m-%d")] = vals
        
            # Fusión
            series_solar_7={}
            cur=ini
            while cur<fin:
                lbl=cur.strftime("%Y-%m-%d")
                if lbl in series_ieod_solar: series_solar_7[lbl]=series_ieod_solar[lbl][:48]
                cur+=timedelta(days=1)
            lbl_fin=fin.strftime("%Y-%m-%d")
            if lbl_fin in series_sol_dia: series_solar_7[lbl_fin]=series_sol_dia[lbl_fin][:48]
        
            if series_solar_7:
                # Promedio
                fechas_lbl=[]; promedios=[]; cur=ini
                while cur<=fin:
                    l=cur.strftime("%Y-%m-%d")
                    if l in series_solar_7:
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_solar_7[l][:48]]
                        promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                    cur+=timedelta(days=1)
                if promedios:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars=ax.bar(fechas_lbl, promedios)
                    for r,v in zip(bars, promedios):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MW"); ax.set_title("HISTÓRICO SOLAR (Potencia Promedio Diario)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
                # Energía diaria (Σ/2)
                fechas_lbl=[]; energia=[]; cur=ini
                while cur<=fin:
                    l=cur.strftime("%Y-%m-%d")
                    if l in series_solar_7:
                        vals=[0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in series_solar_7[l][:48]]
                        energia.append(sum(vals)/2.0); fechas_lbl.append(l)
                    cur+=timedelta(days=1)
                if energia:
                    fig, ax = plt.subplots(figsize=(11,5))
                    bars=ax.bar(fechas_lbl, energia)
                    for r,v in zip(bars, energia):
                        ax.text(r.get_x()+r.get_width()/2, r.get_height(), f"{v:.0f}", ha="center", va="bottom", fontsize=9)
                    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    ax.set_ylabel("MWh"); ax.set_title("HISTÓRICO SOLAR (Energía Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4)
                    plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass
                
        render_graficos_a_pdf(ini=ini, fin=fin, barras=barras, rdo_letras=rdo_letras, work_dir=work_dir, pdf=pdf)
        pdf.close()
        
    try:
        pdf_bytes = (work_dir / "Reporte.pdf").read_bytes()
        st.download_button("Descargar PDF", pdf_bytes, "Reporte.pdf", "application/pdf", type="primary")
    except Exception:
        pass 
        
st.caption("© Reporte Programa Diario de Operación - USGE")
