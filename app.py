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
def _parse_monto_to_float(texto):
    if not texto: return None
    s = str(texto)
    m = re.search(r'COSTO\s*TOTAL\s*:\s*(.+)', s, flags=re.IGNORECASE)
    s = m.group(1) if m else s
    m2 = re.search(r'[-+]?[0-9][0-9\.\,\s]*', s)
    if not m2: return None
    num = m2.group(0).strip().replace(' ', '')
    if '.' in num and ',' in num:
        if num.rfind(',') > num.rfind('.'): num = num.replace('.', '').replace(',', '.')
        else: num = num.replace(',', '')
    elif ',' in num and '.' not in num:
        num = num.replace('.', '').replace(',', '.')
    else:
        num = num.replace(',', '')
    try: return float(num)
    except Exception: return None

def _descargar_si_falta_motivo_local(y, m, d, M, destino, L):
    out = destino / f"Reprog_{y}{m}{d}_{L}.xlsx"
    if out.exists(): return out
    url = base_motivo.format(y=y, m=m, M=M, d=d, dd=d, mm=m, L=L)
    try:
        r = requests.get(url, timeout=40)
        if r.status_code == 200 and r.content.startswith(b"PK"): out.write_bytes(r.content); return out
    except Exception: pass
    return None

def recolectar_costos_totales_pairs(y, m, d, M, destino, letras="ABCDEF"):
    pares = []
    for L in letras:
        p = _descargar_si_falta_motivo_local(y, m, d, M, destino, L)
        if p is None: continue
        df = _leer_excel_motivo(p)
        if df is None or df.empty: continue
        try: colC = df.iloc[:, 2].astype(str)
        except Exception: continue
        idxs = colC[colC.str.contains(r'COSTO\s*TOTAL\s*:', case=False, na=False, regex=True)].index.tolist()
        if not idxs: continue
        monto = _parse_monto_to_float(df.iat[idxs[0], 2])
        if monto is not None: pares.append((L, monto))
    return pares

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
    ax.set_title(titulo); ax.set_xlabel("Hora"); ax.set_ylabel("Índice")
    ax.legend()
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
    min_y = max(0, math.floor(min(valores_plot)) - 10)
    max_y = math.ceil(max(valores_plot)) + 10
    ax.set_ylim(min_y, max_y)
    ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
    ax.grid(axis="y", linestyle="--", alpha=0.5)
    ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=10)
    ax.set_title(f"CMG {barra}")
    ax.set_xlabel("Hora"); ax.set_ylabel("USD/MWh")
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
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs(["Demanda", "Motivos, Costo Total e Índices", "Hidro, Eólico y Solar", "CMG" , "Histórico del IEOD","Histórico de Potencia y Energía"])
    
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
                ax.set_title("DEMANDA"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); demanda_figs1.append(fig)
        
            # === ERRORES relativos absolutos ===
            try:
                def _nz(x):
                    try:
                        if x is None: return 0.0
                        v = float(x)
                        if not isfinite(v) or isnan(v): return 0.0
                        return v
                    except Exception:
                        return 0.0
        
                def _rel_err_abs_pct(den, num):
                    d_ = _nz(den); n_ = _nz(num)
                    if d_ == 0.0: return 0.0
                    return abs((n_ - d_) / d_) * 100.0
        
                def _omit_0_100(v, tol=1e-9):
                    try:
                        f = float(v)
                        if not isfinite(f): return np.nan
                        if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol: return np.nan
                        return f
                    except Exception:
                        return np.nan
        
                if series_dem:
                    orden_series_dem = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_dem]
                    pares_dem = [(orden_series_dem[i], orden_series_dem[i+1]) for i in range(len(orden_series_dem)-1)]
                    errores_dem = {}
                    for idx, (ante, act) in enumerate(pares_dem, start=1):
                        va, vb = series_dem[ante], series_dem[act]
                        mL = min(len(va), len(vb), 48)
                        if mL <= 0: 
                            continue
                        errores_dem[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                    if errores_dem:
                        # Longitud efectiva
                        L = min(len(v) for v in errores_dem.values() if v) or len(horas)
                        x = np.arange(L)
        
                        fig, ax = plt.subplots(figsize=(11, 5))
                        ydata = []
                        for k in sorted(errores_dem.keys(), key=lambda s: int(s.replace("error", ""))):
                            serie = np.array([_omit_0_100(v) for v in errores_dem[k][:L]], dtype=float)
                            ydata.extend(serie[~np.isnan(serie)])
                            ax.plot(x, serie, marker='o', linewidth=2, label=k)
        
                        # Aplica exactamente el mismo formato de la primera gráfica
                        aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas, y_values=ydata, ypad=0.05, xpad=0.5)
        
                        ax.grid(axis="y", linestyle="--", alpha=0.5)
                        ax.set_title("Error Porcentual de DEMANDA")
                        ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                        ax.legend(); plt.tight_layout()
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
        # ====================== MOTIVOS ==========================
        # =========================================================
        try:
            col_tabla, col_graf = st.columns([3, 2])  # ajusta proporciones 3:2 a tu gusto
        
            with col_tabla:
                df_motivos_vista = st.session_state.get("df_motivos")
                if df_motivos_vista is not None and not df_motivos_vista.empty:
                    st.markdown("### Motivo de Reprograma Diario")
                    st.dataframe(df_motivos_vista, use_container_width=True)
        
            with col_graf:
                st.markdown("### Costo Total por Reprograma")
                costos = recolectar_costos_totales_pairs(y=y, m=m, d=d, M=M, destino=work_dir, letras="".join(rdo_letras))
                if costos:
                    etiquetas = [L for (L, _) in costos]; valores = [v for (_, v) in costos]
                    fig, ax = plt.subplots(figsize=(8, 4))
                    ax.bar(etiquetas, valores)
                    ymax = max(valores) if valores else 0
                    ax.set_ylim(0, ymax * 1.15 if ymax > 0 else 1)
                    ax.set_xlabel("Reprograma"); ax.set_ylabel("Costo total (S/)")
                    ax.set_title("Costo Total por Reprograma")
                    try:
                        ax.yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))
                    except Exception:
                        pass
                    for x_, v in zip(etiquetas, valores):
                        ax.annotate(f"{v:,.0f}", xy=(x_, v), xytext=(0, 3),
                                    textcoords="offset points", ha="center", va="bottom", fontsize=8)
                    plt.tight_layout()
                    st.pyplot(fig, use_container_width=True)
                    plt.close(fig)
        except Exception:
            pass
        
        # =====================================================
        # ==================== ÍNDICES ========================
        # =====================================================
        try:
            st.markdown("### Índices")
            res_idx = extraer_listas_alfa_beta_gamma_ultimo(y, m, d, M, work_dir)
            if res_idx.get("reprograma"):
                xlbls = _build_halfhour_labels()
                alfa  = _pad_or_trim_48(res_idx.get("alfa"))
                beta  = _pad_or_trim_48(res_idx.get("beta"))
                gamma = _pad_or_trim_48(res_idx.get("gamma"))
                c1, c2, c3 = st.columns(3)
                with c1: 
                    fig = _plot_series(xlbls, alfa, "Alfa")
                    st.pyplot(fig)
                    plt.close(fig)
                with c2: 
                    fig = _plot_series(xlbls, beta, "Beta")
                    st.pyplot(fig)
                    plt.close(fig)
                with c3: 
                    fig = _plot_series(xlbls, gamma, "Gamma")
                    st.pyplot(fig)
                    plt.close(fig)
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
                ax.set_title("HIDRO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); hidro_figs1.append(fig)
        
        except Exception:
            pass
        
        # ==================== ERROR HIDRO ======================
        try:
            def _nz(x):
                try:
                    if x is None: return 0.0
                    v = float(x)
                    if not isfinite(v) or isnan(v): return 0.0
                    return v
                except Exception:
                    return 0.0
        
            def _rel_err_abs_pct(den, num):
                d_ = _nz(den); n_ = _nz(num)
                if d_ == 0.0: return 0.0
                return abs((n_ - d_) / d_) * 100.0
        
            def _omit_0_100(v, tol=1e-9):
                try:
                    f = float(v)
                    if not isfinite(f): return np.nan
                    if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol: return np.nan
                    return f
                except Exception:
                    return np.nan
        
            if series_h:
                orden_series = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_h]
                pares = [(orden_series[i], orden_series[i+1]) for i in range(len(orden_series)-1)]
                errores = {}
                for idx, (ante, act) in enumerate(pares, start=1):
                    va, vb = series_h[ante], series_h[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
                    errores[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                if errores:
                    L = min(len(v) for v in errores.values() if v) or len(horas)
                    x = np.arange(L)
        
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ydata_e = []
                    for k in sorted(errores.keys(), key=lambda s: int(s.replace("error", ""))):
                        serie = np.array([_omit_0_100(v) for v in errores[k][:L]], dtype=float)
                        ydata_e.extend(serie[~np.isnan(serie)])
                        ax.plot(x, serie, marker='o', linewidth=2, label=k)
        
                    # Misma X que HIDRO (y DEMANDA) + aire en Y
                    aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas,
                                       y_values=ydata_e, ypad=0.05, xpad=0.5)
        
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_title("Error Porcentual de HIDRO")
                    ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                    ax.legend(); plt.tight_layout()
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
                ax.set_title("EÓLICO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); eolica_figs1.append(fig)
        
            # ============== Error EÓLICA ==============
            try:
                def _nz(x):
                    try:
                        if x is None: return 0.0
                        v = float(x)
                        if not isfinite(v) or isnan(v): return 0.0
                        return v
                    except Exception:
                        return 0.0
        
                def _rel_err_abs_pct(den, num):
                    d_ = _nz(den); n_ = _nz(num)
                    if d_ == 0.0: return 0.0
                    return abs((n_ - d_) / d_) * 100.0
        
                def _omit_0_100(v, tol=1e-9):
                    try:
                        f = float(v)
                        if not isfinite(f): return np.nan
                        if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol: return np.nan
                        return f
                    except Exception:
                        return np.nan
        
                if series_rer:
                    orden_series_eol = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_rer]
                    pares_eol = [(orden_series_eol[i], orden_series_eol[i+1]) for i in range(len(orden_series_eol)-1)]
                    errores_eol = {}
                    for idx, (ante, act) in enumerate(pares_eol, start=1):
                        va, vb = series_rer[ante], series_rer[act]
                        mL = min(len(va), len(vb), 48)
                        if mL <= 0:
                            continue
                        errores_eol[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                    if errores_eol:
                        L = min(len(v) for v in errores_eol.values() if v) or len(horas)
                        x = np.arange(L)
        
                        fig, ax = plt.subplots(figsize=(11, 5))
                        ydata = []
                        for k in sorted(errores_eol.keys(), key=lambda s: int(s.replace("error", ""))):
                            serie = np.array([_omit_0_100(v) for v in errores_eol[k][:L]], dtype=float)
                            ydata.extend(serie[~np.isnan(serie)])
                            ax.plot(x, serie, marker='o', linewidth=2, label=k)
        
                        # Igual formato de ejes que la curva EÓLICA
                        aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas,
                                           y_values=ydata, ypad=0.05, xpad=0.5)
        
                        ax.grid(axis="y", linestyle="--", alpha=0.5)
                        ax.set_title("Error Porcentual de EÓLICO")
                        ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                        ax.legend(); plt.tight_layout()
                        eolica_figs1.append(fig)
            except Exception:
                pass
        
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
                ax.set_title("SOLAR"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
                plt.tight_layout(); solar_figs1.append(fig)
        
            # Error Solar
            try:
                def _nz(x):
                    try:
                        if x is None: return 0.0
                        v = float(x)
                        if not isfinite(v) or isnan(v): return 0.0
                        return v
                    except Exception:
                        return 0.0
        
                def _rel_err_abs_pct(den, num):
                    d_ = _nz(den); n_ = _nz(num)
                    if d_ == 0.0: return 0.0
                    return abs((n_ - d_) / d_) * 100.0
        
                def _omit_0_100(v, tol=1e-9):
                    try:
                        f = float(v)
                        if not isfinite(f): return np.nan
                        if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol: return np.nan
                        return f
                    except Exception:
                        return np.nan
        
                if series_sol:
                    orden_series_sol = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_sol]
                    pares_sol = [(orden_series_sol[i], orden_series_sol[i+1]) for i in range(len(orden_series_sol)-1)]
                    errores_sol = {}
                    for idx, (ante, act) in enumerate(pares_sol, start=1):
                        va, vb = series_sol[ante], series_sol[act]
                        mL = min(len(va), len(vb), 48)
                        if mL <= 0:
                            continue
                        errores_sol[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                    if errores_sol:
                        L = min(len(v) for v in errores_sol.values() if v) or len(horas)
                        x = np.arange(L)
        
                        fig, ax = plt.subplots(figsize=(11, 5))
                        ydata = []
                        for k in sorted(errores_sol.keys(), key=lambda s: int(s.replace("error", ""))):
                            serie = np.array([_omit_0_100(v) for v in errores_sol[k][:L]], dtype=float)
                            ydata.extend(serie[~np.isnan(serie)])
                            ax.plot(x, serie, marker='o', linewidth=2, label=k)
        
                        # Misma X que el gráfico SOLAR + “aire” en Y
                        aplicar_formato_xy(ax, L=L, ticks_pos=ticks_pos, horas=horas,
                                           y_values=ydata, ypad=0.05, xpad=0.5)
        
                        ax.grid(axis="y", linestyle="--", alpha=0.5)
                        ax.set_title("Error Porcentual de SOLAR")
                        ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                        ax.legend(); plt.tight_layout()
                        solar_figs1.append(fig)
            except Exception:
                pass
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
            cols = st.columns(len(barras))
            for i, barra in enumerate(barras):
                with cols[i]:
                    df_pdo = cargar_dataframe(pdo_res, stem_file)
                    datosPDO = rellenar_hasta_48(extraer_columna(df_pdo, barra))
                    if not datosPDO: 
                        continue
                    series_barra = {"PDO": datosPDO}
                    for letra in rdo_letras:
                        rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
                        df_rdo   = cargar_dataframe(rdo_res, stem_file)
                        datosRDO = rellenar_hasta_48(extraer_columna(df_rdo, barra))
                        if datosRDO: series_barra[f"RDO {letra}"] = datosRDO
                    fig, ax = plt.subplots(figsize=(11, 5))
                    ok = _plot_cmg_barra_en_axes(ax, barra, series_barra, horas, ticks_pos, ticks_lbl)
                    if ok: plt.tight_layout(); st.pyplot(fig); plt.close(fig)
        except Exception:
            pass
    
    with tab5:
        # =========================================================
        # ==================== HISTORICO HIDRO ====================
        # =========================================================
        st.markdown("### HIDRO")
        hidro_figs2 = []
        # Histórico IEOD (HIDRO)
        try:    
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
        
            #if series_por_dia:
                #fig, ax = plt.subplots(figsize=(12, 6))
                #xs = list(range(48))
                #for lbl, v_sum in series_por_dia:
                    #ax.plot(xs, v_sum, marker="o", linewidth=2, label=lbl)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO HIDRO DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha"); plt.tight_layout()
                #hidro_figs2.append(fig)
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
            #if series_dia:
                #fig, ax = plt.subplots(figsize=(11,5)); y_plot=[]
                #for fecha_lbl, valores in series_dia.items():
                    #xlab, yv = recortar_ceros_inicio(valores, horas)
                    #if not yv: continue
                    #y_plot.extend(yv); ax.plot(xlab, yv, marker="o", linewidth=2, label=fecha_lbl)
                #if y_plot:
                    #min_y = max(0, math.floor(min(y_plot)) - 10); max_y = math.ceil(max(y_plot)) + 10
                    #ax.set_ylim(min_y, max_y); ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                    #ax.grid(axis="y", linestyle="--", alpha=0.5)
                    #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                    #ax.set_title("HISTÓRICO HIDRO DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                    #ax.legend(title="Fecha"); plt.tight_layout()
                    #hidro_figs2.append(fig)
        except Exception:
            pass
    
        # Fusión 
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
                ax.set_title("HISTÓRICO HIDRO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout(); hidro_figs2.append(fig)
        except Exception:
            pass
        
        # Mostrar todas las HIDRO en UNA FILA
        if hidro_figs2:
            cols = st.columns(len(hidro_figs2))
            for i, fig in enumerate(hidro_figs2):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
        
        # =========================================================
        # ================== HISTORICO DEMANDA ====================
        # =========================================================
        st.markdown("### DEMANDA")
        demanda_figs2 = []
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
            #if series_ieod_dem:
                #fig, ax = plt.subplots(figsize=(12, 6)); xs=list(range(48))
                #for lbl in sorted(series_ieod_dem.keys()):
                    #v = [0 if (vi is None or (isinstance(vi,float) and math.isnan(vi))) else vi for vi in series_ieod_dem[lbl][:48]]
                    #ax.plot(xs, v, marker="o", linewidth=2, label=lbl)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO DEMANDA DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha")
                #plt.tight_layout(); demanda_figs2.append(fig)
            
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
            #if series_dia:
                #fig, ax = plt.subplots(figsize=(11, 5)); y_all=[]
                #for fecha_lbl, valores in series_dia.items():
                    #xlab, yv = recortar_ceros_inicio(valores, horas)
                    #if not yv: continue
                    #y_all.extend(yv); ax.plot(xlab, yv, marker="o", linewidth=2, label=fecha_lbl)
                #if y_all:
                    #ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                #ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                #ax.grid(axis="y", linestyle="--", alpha=0.5)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO DEMANDA DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.legend(title="Fecha"); plt.tight_layout(); demanda_figs2.append(fig)
    
            # Fusión
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
                ax.set_title("HISTÓRICO DEMANDA"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout(); demanda_figs2.append(fig)
        except Exception:
            pass
            
        if demanda_figs2:
            cols = st.columns(len(demanda_figs2))
            for i, fig in enumerate(demanda_figs2):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
        
        # =========================================================
        # =================== HISTORICO EÓLICO ====================
        # =========================================================
        st.markdown("### EÓLICA")
        eolica_figs2 = []
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
            #if series_ieod_eol:
                #fig, ax = plt.subplots(figsize=(12, 6)); xs=list(range(48))
                #for lbl in sorted(series_ieod_eol.keys()):
                    #v = [0 if (vi is None or (isinstance(vi,float) and math.isnan(vi))) else vi for vi in series_ieod_eol[lbl][:48]]
                    #ax.plot(xs, v, marker="o", linewidth=2, label=lbl)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO EÓLICO DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha"); plt.tight_layout()
                #eolica_figs2.append(fig)
    
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
            #if series_eol_dia:
                #fig, ax = plt.subplots(figsize=(11, 5)); y_all=[]
                #for fecha_lbl, vals in series_eol_dia.items():
                    #xlab, yv = recortar_ceros_inicio(vals, horas)
                    #if not yv: continue
                    #y_all.extend(yv); ax.plot(xlab, yv, marker="o", linewidth=2, label=fecha_lbl)
                #if y_all:
                    #ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                #ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                #ax.grid(axis="y", linestyle="--", alpha=0.5)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO EÓLICO DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.legend(title="Fecha"); plt.tight_layout(); eolica_figs2.append(fig)
    
            # Fusión 
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
                ax.set_title("HISTÓRICO EÓLICO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout(); eolica_figs2.append(fig)
        except Exception:
            pass
    
        if eolica_figs2:
            cols = st.columns(len(eolica_figs2))
            for i, fig in enumerate(eolica_figs2):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
        
        # =========================================================
        # ==================== HISTORICO SOLAR ====================
        # =========================================================
        st.markdown("### SOLAR")
        solar_figs2 = []
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
            #if series_ieod_solar:
                #fig, ax = plt.subplots(figsize=(12, 6)); xs=list(range(48))
                #for lbl in sorted(series_ieod_solar.keys()):
                    #v = [0 if (vi is None or (isinstance(vi,float) and math.isnan(vi))) else vi for vi in series_ieod_solar[lbl][:48]]
                    #ax.plot(xs, v, marker="o", linewidth=2, label=lbl)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO SOLAR DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha"); plt.tight_layout()
                #solar_figs2.append(fig)
    
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
            #if series_sol_dia:
                #fig, ax = plt.subplots(figsize=(11, 5)); y_all=[]
                #for fecha_lbl, raw_vals in series_sol_dia.items():
                    #y_vals=[]
                    #for i, v in enumerate(raw_vals[:48]):
                        #v = 0 if pd.isna(v) else v
                        #if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                            #y_vals.append(None)
                        #else:
                            #y_vals.append(v)
                    #if all(v is None for v in y_vals): continue
                    #y_all.extend([v for v in y_vals if v is not None])
                    #ax.plot(horas, y_vals, marker="o", linewidth=2, label=fecha_lbl)
                #if y_all:
                    #ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
                #ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                #ax.grid(axis="y", linestyle="--", alpha=0.5)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO SOLAR DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.legend(title="Fecha"); plt.tight_layout(); solar_figs2.append(fig)
    
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
                ax.set_title("HISTÓRICO SOLAR"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.legend(title="Fecha"); plt.tight_layout(); solar_figs2.append(fig)
        except Exception:
            pass
    
        if solar_figs2:
            cols = st.columns(len(solar_figs2))
            for i, fig in enumerate(solar_figs2):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)
        
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
                xs = list(range(48)); bw = 0.4; x_pas=[i-bw/2 for i in xs]; x_reg=[i+bw/2 for i in xs]
                ax.bar(x_pas, v_pas, width=bw, label="H. PASADA")
                ax.bar(x_reg, v_reg, width=bw, label="H. REGULACION")
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="center", fontsize=8)
                ax.set_title("H. PASADA - H. REGULACIÓN"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(); plt.tight_layout()
                hidro_figs.append(fig)
        except Exception:
            pass
    
        # Histórico IEOD (HIDRO)
        try:    
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
                ax.set_title("HISTÓRICO HIDRO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MWh")
                    ax.set_title("HISTÓRICO HIDRO (Energía Máxima Diaria)")
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
                ax.set_title("HISTÓRICO DEMANDA"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MW"); ax.set_title("HISTÓRICO DEMANDA (Potencia Promedio Diario)")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MW"); ax.set_title("HISTÓRICO DEMANDA (Máxima Diaria)")
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
                ax.set_title("HISTÓRICO EÓLICO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
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
    
                        ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MWh")
                    ax.set_title("HISTÓRICO EÓLICO (Energía Máxima Diaria)")
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
                ax.set_title("HISTÓRICO SOLAR"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
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
                    ax.set_xlabel("Fecha"); ax.set_ylabel("MWh")
                    ax.set_title("HISTÓRICO SOLAR (Energía Máxima Diaria)")
                    ax.grid(axis="y", linestyle="--", alpha=0.4); plt.tight_layout(); solar_figs.append(fig)
        except Exception:
            pass
    
        if solar_figs:
            cols = st.columns(len(solar_figs))
            for i, fig in enumerate(solar_figs):
                with cols[i]: 
                    st.pyplot(fig)
                plt.close(fig)

# -----------------------------------------------------------------------------
# ------------------------------------ PDF ------------------------------------
# -----------------------------------------------------------------------------        
def render_graficos_a_pdf(ini: date, fin: date, barras: list[str], rdo_letras: list[str], work_dir: Path, pdf: PdfPages):
    return    

st.set_page_config(page_title="Reporte Programa Diario de Operación", layout="wide")
st.sidebar.header("Parámetros")
fecha_sel = st.sidebar.date_input("Fecha del reporte", value=date.today(), format="DD/MM/YYYY")
ini = st.sidebar.date_input("Inicio del rango", value=fecha_sel, format="DD/MM/YYYY")
barras = BARRAS_DEF
rdo_letras = RDO_LETRAS_DEF
fin = fecha_sel
work_dir_str = st.sidebar.text_input("Carpeta de trabajo", value=str(Path.home() / "Descargas_T"))
work_dir = Path(work_dir_str); work_dir.mkdir(parents=True, exist_ok=True)
gen_generar = st.sidebar.button("Generar", type="primary")

st.title("Reporte Programa Diario de Operación")
y, m, d = fecha_sel.year, f"{fecha_sel.month:02d}", f"{fecha_sel.day:02d}"
M = MES_TXT[int(m) - 1]
fecha_hum = fecha_sel.strftime("%d/%m/%Y")
now_str = datetime.now().strftime("%H:%M")
fecha_str = f"{y}{m}{d}"
ddmm = f"{d}{m}"

st.subheader(f"Reporte del {fecha_hum}")
st.caption(f"Actualizado a las {now_str} horas")

btn_cols = st.columns([1, 8])

if gen_generar:
    with st.spinner("Descargando MOTIVOS RDO…"):
        df_motivos_local = recolectar_motivos_dia(y=y, m=m, d=d, M=M, destino=work_dir, letras="".join(rdo_letras))
        st.session_state["df_motivos"] = df_motivos_local

    with st.spinner("Descargando insumos PDO/RDO para CMG…"):
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
        ax.text(0.5, 0.4, f"Generado: {datetime.now().strftime('%d/%m/%Y %H:%M')}", ha="center", va="center", fontsize=12)
        pdf.savefig(fig); plt.close(fig)

        # Tabla MOTIVOS
        if df_motivos_local is not None and not df_motivos_local.empty:
            fig, ax = plt.subplots(figsize=(11, 1 + 0.6 * len(df_motivos_local))); ax.axis("off")
            tabla = ax.table(cellText=df_motivos_local.values, colLabels=df_motivos_local.columns,
                             loc="center", cellLoc="left", colWidths=[0.14, 0.12, 0.12, 0.62])
            for (r,c), cell in tabla.get_celld().items():
                if c in (0,1,2): cell.get_text().set_ha('center')
                elif c == 3:     cell.get_text().set_ha('left')
            tabla.auto_set_font_size(False); tabla.set_fontsize(8); tabla.scale(1, 1.2)
            ax.set_title("Motivo de Reprograma Diario", pad=12, fontsize=11)
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Costo Total (PDF)
        costos_pdf = recolectar_costos_totales_pairs(y=y, m=m, d=d, M=M, destino=work_dir, letras="".join(rdo_letras))
        if costos_pdf:
            etiquetas = [L for (L, _) in costos_pdf]; valores = [v for (_, v) in costos_pdf]
            fig, ax = plt.subplots(figsize=(8, 4))
            ax.bar(etiquetas, valores)
            ymax = max(valores) if valores else 0
            ax.set_ylim(0, ymax * 1.15 if ymax > 0 else 1)
            ax.set_xlabel("Reprograma"); ax.set_ylabel("Costo total (S/)")
            ax.set_title("Costo Total por Reprograma")
            try: ax.yaxis.set_major_formatter(mticker.StrMethodFormatter('{x:,.0f}'))
            except Exception: pass
            for x1, v1 in zip(etiquetas, valores):
                ax.annotate(f"{v1:,.0f}", xy=(x1, v1), xytext=(0, 3),
                            textcoords="offset points", ha="center", va="bottom", fontsize=8)
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Índices (PDF)
        res_idx = extraer_listas_alfa_beta_gamma_ultimo(y, m, d, M, work_dir)
        if res_idx.get("reprograma"):
            xlbls = _build_halfhour_labels()
            alfa  = _pad_or_trim_48(res_idx.get("alfa"))
            beta  = _pad_or_trim_48(res_idx.get("beta"))
            gamma = _pad_or_trim_48(res_idx.get("gamma"))
            _plot_series_pdf(xlbls, alfa,  f"Índices {res_idx['reprograma']} – Alfa",  pdf)
            _plot_series_pdf(xlbls, beta,  f"Índices {res_idx['reprograma']} – Beta",  pdf)
            _plot_series_pdf(xlbls, gamma, f"Índices {res_idx['reprograma']} – Gamma", pdf)

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

        # HIDRO (PDF)
        barras_rer = ["CARPAPATA","LA JOYA","STACRUZ12","HUASAHUASI","RONCADOR","PURMACANA","NIMPERIAL","PIZARRAS",
                      "POECHOS2","CANCHAYLLO","CHANCAY","RUCUY","RUNATULLOII","RUNATULLOIII","YANAPAMPA","POTRERO",
                      "CH MARANON","YARUCAYA","CHHER1","CHANGELI","CHANGELII","CHANGELIII","8AGOSTO","RENOVANDESH1",
                      "EL CARMEN","CH MANTA","SANTA ROSA 1","SANTA ROSA 2","TUPURI","CH HUALLIN"]
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
            tr = rellenar_hasta_48(totales_rer (cargar_dataframe(rdo_res, stem_rer), [x.upper() for x in barras_rer]))
            if th and tr: series_h[f"RDO {letra}"] = suma_elementos(th, tr)

        if series_h:
            fig, ax = plt.subplots(figsize=(11, 5))
            valores_plot = []
            for nombre, valores in series_h.items():
                xlab, yv = recortar_ceros_inicio(valores, horas)
                if not yv: continue
                valores_plot.extend(yv)
                ax.plot(xlab, yv, marker="o", linewidth=2, label=nombre)
            if valores_plot:
                min_y = max(0, math.floor(min(valores_plot)) - 10)
                max_y = math.ceil(max(valores_plot)) + 10
                ax.set_ylim(min_y, max_y)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                ax.set_title("HIDRO")
                ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.legend(); plt.tight_layout(); pdf.savefig(fig)
            plt.close(fig)
            
        # ERROR HIDRO (PDF)
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
            d = _nz(den); n = _nz(num)
            if d == 0.0:
                return 0.0
            return abs((n - d) / d) * 100.0
        
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
        
        if series_h:
            orden_series = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_h]
            pares = [(orden_series[i], orden_series[i+1]) for i in range(len(orden_series)-1)]
        
            errores = {}
            for idx, (ante, act) in enumerate(pares, start=1):
                va, vb = series_h[ante], series_h[act]
                m = min(len(va), len(vb), 48)
                if m > 0:
                    errores[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(m)]
        
            if errores:
                L = min(len(v) for v in errores.values() if v) or 48
                x = np.arange(L)
        
                # Etiquetas de tiempo (alineadas con tus marcas cada 1h)
                tpos = [i for i in ticks_pos if i < L]
                tlbl = [horas[i] for i in tpos]
        
                fig, ax = plt.subplots(figsize=(11, 5))
                for k in sorted(errores.keys(), key=lambda s: int(s.replace("error", ""))):
                    y = np.array([_omit_0_100(v) for v in errores[k][:L]], dtype=float)
                    ax.plot(x, y, marker='o', linewidth=2, label=k)
        
                ax.set_title("Error Porcentual de HIDRO")
                ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                ax.grid(axis="y", linestyle="--", alpha=0.5)
                ax.set_xticks(tpos); ax.set_xticklabels(tlbl, rotation=90, ha="right", fontsize=8)
                ax.legend()
                plt.tight_layout()
                pdf.savefig(fig)
                plt.close(fig)

        # H. Pasada vs H. Regulación (IEOD día fin)
        def _norm(txt): 
            import re; return re.sub(r"\s+"," ",str(txt).strip()).upper()
        def _find_cols(cols):
            c_pas = c_reg = None
            for c in cols:
                k = _norm(c)
                if k == "H. PASADA" and c_pas is None: c_pas = c
                if k == "H. REGULACION" and c_reg is None: c_reg = c
            return c_pas, c_reg
        def _lee_ieod_bytes(y2, m2, M2, d2):
            ddmm2 = f"{d2:02d}{m2:02d}"
            url  = base_ieod.format(y=y2, m=f"{m2:02d}", M=M2, d=f"{d2:02d}", ddmm=ddmm2)
            r = requests.get(url, timeout=40); r.raise_for_status()
            return io.BytesIO(r.content)
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
        try:
            fbytes = _lee_ieod_bytes(y2, m2, M2, d2)
            with open(work_dir / f"IEOD_AnexoA_{f.strftime('%Y%m%d')}.xlsx","wb") as w: w.write(fbytes.getbuffer())
            v_pas, v_reg, v_sum = _extrae_listas_48(fbytes)
            if v_sum is not None:
                fig, ax = plt.subplots(figsize=(12, 5))
                x = list(range(48)); bw = 0.4; x_pas=[i-bw/2 for i in x]; x_reg=[i+bw/2 for i in x]
                ax.bar(x_pas, v_pas, width=bw, label="H. PASADA")
                ax.bar(x_reg, v_reg, width=bw, label="H. REGULACION")
                ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                ax.set_title("H. PASADA - H. REGULACIÓN"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend()
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass

        # Histórico IEOD (ini..fin)
        series_por_dia = []
        dias = (fin - ini).days + 1
        for k in range(dias):
            f = ini + timedelta(days=k)
            y2, m2, d2 = f.year, f.month, f.day; M2 = MES_TXT[m2-1]
            try:
                fbytes = _lee_ieod_bytes(y2, m2, M2, d2)
                with open(work_dir / f"IEOD_AnexoA_{f.strftime('%Y%m%d')}.xlsx","wb") as w: w.write(fbytes.getbuffer())
                _, _, v_sum = _extrae_listas_48(fbytes)
                if v_sum is not None:
                    series_por_dia.append((f.strftime("%Y-%m-%d"), v_sum))
            except Exception:
                continue
        #if series_por_dia:
            #fig, ax = plt.subplots(figsize=(12, 6)); x = list(range(48))
            #for lbl, v_sum in series_por_dia:
                #ax.plot(x, v_sum, marker="o", linewidth=2, label=lbl)
            #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO HIDRO DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha")
            #plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Histórico HIDRO de RPO A (ini..fin)
        series_dia = {}
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
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                except Exception:
                    continue
            th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
            tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer]))
            if th and tr: series_dia[f.isoformat()] = suma_elementos(th, tr)
        #if series_dia:
            #fig, ax = plt.subplots(figsize=(11, 5)); y_plot=[]
            #for fecha_lbl, valores in series_dia.items():
                #xlab, yv = recortar_ceros_inicio(valores, horas)
                #if not yv: continue
                #y_plot.extend(yv); ax.plot(xlab, yv, marker="o", linewidth=2, label=fecha_lbl)
            #if y_plot:
                #min_y = max(0, math.floor(min(y_plot)) - 10); max_y = math.ceil(max(y_plot)) + 10
                #ax.set_ylim(min_y, max_y); ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                #ax.grid(axis="y", linestyle="--", alpha=0.5)
                #ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
                #ax.set_title("HISTÓRICO HIDRO DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
                #ax.legend(title="Fecha"); plt.tight_layout(); pdf.savefig(fig)
            #plt.close(fig)

        # Fusión (IEOD 6 días + RDO A último día)
        series_7 = {}
        ini_ieod = ini; fin_ieod = fin - timedelta(days=1)
        dias_ieod = (fin_ieod - ini_ieod).days + 1 if fin_ieod >= ini_ieod else 0
        for k in range(dias_ieod):
            f = ini_ieod + timedelta(days=k)
            y2, m2, d2 = f.year, f.month, f.day; M2 = MES_TXT[m2-1]
            try:
                fbytes = _lee_ieod_bytes(y2, m2, M2, d2)
                with open(work_dir / f"IEOD_AnexoA_{f.strftime('%Y%m%d')}.xlsx","wb") as w: w.write(fbytes.getbuffer())
                _, _, v_sum = _extrae_listas_48(fbytes)
                if v_sum is not None: series_7[f.strftime("%Y-%m-%d")] = v_sum
            except Exception:
                continue
        f_last = fin; yk, mk, dk = f_last.year, f_last.strftime("%m"), f_last.strftime("%d")
        M_TXT = MES_TXT[f_last.month-1]
        carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
        resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
        if not resultados.exists():
            try:
                r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
            except Exception:
                pass
        th = rellenar_hasta_48(totales_hidro(cargar_dataframe(resultados, stem_hidro)))
        tr = rellenar_hasta_48(totales_rer (cargar_dataframe(resultados, stem_rer), [x.upper() for x in barras_rer]))
        if th and tr: series_7[f_last.strftime("%Y-%m-%d")] = suma_elementos(th, tr)

        if series_7:
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
            ax.set_xticks(ticks_pos); ax.set_xticklabels(ticks_lbl, rotation=90, ha="right", fontsize=8)
            if y_all:
                y_min = max(0, math.floor(min(y_all)) - 10); y_max = math.ceil(max(y_all)) + 10
                ax.set_ylim(y_min, y_max)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_title("HISTÓRICO HIDRO"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            ax.legend(title="Fecha"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

            # Promedio diario de HIDRO (barras)
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
                ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
                ax.set_title("HISTÓRICO HIDRO (Potencia Promedio Diario)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

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
                ax.set_xlabel("Fecha"); ax.set_ylabel("MWh")
                ax.set_title("HISTÓRICO HIDRO (Energía Máxima Diaria)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # DEMANDA (PDF) 
        # Etiquetas de tiempo
        _inicio_d = datetime(2000, 1, 1, 0, 30)
        _horas_d  = [(_inicio_d + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
        _horas_d[-1] = "23:59"
        _ticks_pos_d = list(range(0, 48, 2))
        _ticks_lbl_d = [_horas_d[i] for i in _ticks_pos_d]

        # Rutas de resultados del día FIN
        _pdo_res = work_dir / f"PDO_{fin.strftime('%Y%m%d')}" / f"YUPANA_{fin.strftime('%Y%m%d')}" / "RESULTADOS"

        archivos_dem = {
            "HIDRO"   : "Hidro - Despacho (MW)",
            "TERMICA" : "Termica - Despacho (MW)",
            "RER"     : "Rer y No COES - Despacho (MW)"
        }

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

        # DEMANDA actual (PDO vs RDO A–E)
        series_dem = {}

        vals_hidro_p   = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(_pdo_res, archivos_dem["HIDRO"])))
        vals_termica_p = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(_pdo_res, archivos_dem["TERMICA"])))
        vals_rer_p     = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(_pdo_res, archivos_dem["RER"])))
        series_dem["PDO"] = suma_elementos(vals_hidro_p, vals_termica_p, vals_rer_p)

        for letra in rdo_letras:
            _rdo_res = work_dir / f"RDO_{letra}_{fin.strftime('%Y%m%d')}" / f"YUPANA_{fin.strftime('%d%m')}{letra}" / "RESULTADOS"
            vals_h = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(_rdo_res, archivos_dem["HIDRO"])))
            vals_t = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(_rdo_res, archivos_dem["TERMICA"])))
            vals_r = rellenar_hasta_48(fila_sin_primer_valor(cargar_dataframe(_rdo_res, archivos_dem["RER"])))
            if any((vals_h, vals_t, vals_r)):
                series_dem[f"RDO {letra}"] = suma_elementos(vals_h, vals_t, vals_r)

        if series_dem:
            fig, ax = plt.subplots(figsize=(11, 5))
            yvals = []
            for nombre, valores in series_dem.items():
                xlab, yv = recortar_ceros_inicio(valores, _horas_d)
                if not yv: continue
                yvals.extend(yv)
                ax.plot(xlab, yv, marker="o", linewidth=2, label=nombre)
            if yvals:
                y_min = max(0, math.floor(min(yvals)) - 10)
                y_max = math.ceil(max(yvals)) + 10
                ax.set_ylim(y_min, y_max)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_xticks(_ticks_pos_d); ax.set_xticklabels(_ticks_lbl_d, rotation=90, ha="right", fontsize=8)
            ax.set_title("DEMANDA"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # ERROR DEMANDA (PDF)
        try:
            def _nz(x):
                try:
                    v = float(x) if x is not None else 0.0
                    return v if np.isfinite(v) else 0.0
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
                    if not np.isfinite(f): 
                        return np.nan
                    if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol:
                        return np.nan
                    return f
                except Exception:
                    return np.nan
        
            if series_dem:
                orden_series_dem = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_dem]
                pares_dem = [(orden_series_dem[i], orden_series_dem[i+1]) for i in range(len(orden_series_dem)-1)]
                errores_dem = {}
                for idx, (ante, act) in enumerate(pares_dem, start=1):
                    va, vb = series_dem[ante], series_dem[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0: 
                        continue
                    errores_dem[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                if errores_dem and any(len(v) > 0 for v in errores_dem.values()):
                    L = min(len(v) for v in errores_dem.values() if v)
                    x = np.arange(L)
                    fig, ax = plt.subplots(figsize=(11, 5))
                    for k in sorted(errores_dem.keys(), key=lambda s: int(s.replace("error", ""))):
                        serie = [_omit_0_100(v) for v in errores_dem[k][:L]]
                        ax.plot(x, np.array(serie, dtype=float), marker='o', linewidth=2, label=k)
                    ax.set_title("Error Porcentual de DEMANDA")
                    ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_xticks(x); ax.set_xticklabels(_horas_d[:L], rotation=90, fontsize=8)
                    ax.legend(); plt.tight_layout()
                    pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass

        # HISTÓRICO DEMANDA (IEOD) — ini..fin
        series_ieod_demanda = {}
        cur = ini
        while cur <= fin:
            try:
                fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                vals = _extrae_demanda_48(fb)
                if vals and any(v != 0 for v in vals):
                    series_ieod_demanda[cur.strftime("%Y-%m-%d")] = vals[:48]
            except Exception:
                pass
            cur += timedelta(days=1)

        #if series_ieod_demanda:
            #fig, ax = plt.subplots(figsize=(12, 6))
            #x = list(range(48))
            #for lbl in sorted(series_ieod_demanda.keys()):
                #v = [0 if (vi is None or (isinstance(vi, float) and math.isnan(vi))) else vi for vi in series_ieod_demanda[lbl][:48]]
                #ax.plot(x, v, marker="o", linewidth=2, label=lbl)
            #ax.set_xticks(_ticks_pos_d); ax.set_xticklabels(_ticks_lbl_d, rotation=45, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO DEMANDA DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha")
            #plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # HISTÓRICO DEMANDA (RPO A) — ini..fin
        series_dem_dia = {}
        for k in range((fin - ini).days + 1):
            f = ini + timedelta(days=k)
            yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
            M_TXT = MES_TXT[f.month-1]
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
                series_dem_dia[f.strftime("%Y-%m-%d")] = suma_elementos(vals_h, vals_t, vals_r)

        #if series_dem_dia:
            #fig, ax = plt.subplots(figsize=(11, 5))
            #y_all=[]
            #for fecha_lbl, valores in series_dem_dia.items():
                #xlab, yv = recortar_ceros_inicio(valores, _horas_d)
                #if not yv: continue
                #y_all.extend(yv)
                #ax.plot(xlab, yv, marker="o", linewidth=2, label=fecha_lbl)
            #if y_all:
                #y_min = max(0, math.floor(min(y_all)) - 10)
                #y_max = math.ceil(max(y_all)) + 10
                #ax.set_ylim(y_min, y_max)
            #ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            #ax.grid(axis="y", linestyle="--", alpha=0.5)
            #ax.set_xticks(_ticks_pos_d); ax.set_xticklabels(_ticks_lbl_d, rotation=90, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO DEMANDA DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            #ax.legend(title="Fecha"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # FUSIÓN DEMANDA (IEOD ini..fin-1 + RDO-A fin)
        series_demanda_7 = {}
        cur = ini
        while cur < fin:
            lbl = cur.strftime("%Y-%m-%d")
            if lbl in series_ieod_demanda:
                series_demanda_7[lbl] = series_ieod_demanda[lbl][:48]
            else:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_demanda_48(fb)
                    if vals: series_demanda_7[lbl] = vals[:48]
                except Exception:
                    pass
            cur += timedelta(days=1)

        lbl_fin = fin.strftime("%Y-%m-%d")
        if lbl_fin in series_dem_dia:
            series_demanda_7[lbl_fin] = series_dem_dia[lbl_fin][:48]

        if series_demanda_7:
            fechas_orden = []
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_demanda_7: fechas_orden.append(l)
                cur += timedelta(days=1)
            fig, ax = plt.subplots(figsize=(12, 6))
            x_idx = list(range(48)); y_all=[]
            for l in fechas_orden:
                fobj = datetime.strptime(l, "%Y-%m-%d").date()
                estilo = '--' if fobj < fin else '-'
                vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_demanda_7[l][:48]]
                y_all.extend(vals)
                ax.plot(x_idx, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
            ax.set_xticks(_ticks_pos_d); ax.set_xticklabels(_ticks_lbl_d, rotation=90, ha="right", fontsize=8)
            if y_all:
                y_min = max(0, math.floor(min(y_all)) - 10)
                y_max = math.ceil(max(y_all)) + 10
                ax.set_ylim(y_min, y_max)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_title("HISTÓRICO DEMANDA"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            ax.legend(title="Fecha"); plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Promedio diario (barras)
        if series_demanda_7:
            fechas_lbl=[]; promedios=[]
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_demanda_7:
                    vals = series_demanda_7[l][:48]
                    vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in vals]
                    promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                cur += timedelta(days=1)
            if promedios:
                fig, ax = plt.subplots(figsize=(9,5))
                bars = ax.bar(fechas_lbl, promedios)
                for rect, val in zip(bars, promedios):
                    ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                            ha="center", va="bottom", fontsize=9, clip_on=True)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
                ax.set_title("HISTÓRICO DEMANDA (Potencia Promedio Diario)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Máximo diario (barras)
        if series_demanda_7:
            fechas_lbl=[]; maximos=[]
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_demanda_7:
                    vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v)
                            for v in series_demanda_7[l][:48]]
                    maximos.append(max(vals) if vals else 0.0); fechas_lbl.append(l)
                cur += timedelta(days=1)
            if maximos:
                fig, ax = plt.subplots(figsize=(9,5))
                bars = ax.bar(fechas_lbl, maximos)
                for rect, val in zip(bars, maximos):
                    ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                            ha="center", va="bottom", fontsize=9, clip_on=True)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
                ax.set_title("HISTÓRICO DEMANDA (Máxima Diaria)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # EÓLICA (PDF)
        _inicio_e = datetime(2000, 1, 1, 0, 30)
        _horas_e  = [(_inicio_e + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
        _horas_e[-1] = "23:59"
        _ticks_pos_e = list(range(0, 48, 2))
        _ticks_lbl_e = [_horas_e[i] for i in _ticks_pos_e]

        _pdo_res = work_dir / f"PDO_{fin.strftime('%Y%m%d')}" / f"YUPANA_{fin.strftime('%Y%m%d')}" / "RESULTADOS"

        stem_rer = "Rer y No COES - Despacho (MW)"
        barras_eol = [
            "PE TALARA","PE CUPISNIQUE","PQEEOLICOMARCONA","PQEEOLICO3HERMANAS",
            "WAYRAI","HUAMBOS","DUNA","CE PUNTA LOMITASBL1","CE PUNTA LOMITASBL2",
            "PTALOMITASEXPBL1","PTALOMITASEXPBL2","PE SAN JUAN","WAYRAEXP"
        ]

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

        # EÓLICA actual (PDO vs RDO A–E)
        series_rer = {}
        df_pdo_rer = cargar_dataframe(_pdo_res, stem_rer)
        vals_pdo   = rellenar_hasta_48(totales_rer(df_pdo_rer, [x.upper() for x in barras_eol]))
        if vals_pdo: series_rer["PDO"] = vals_pdo

        for letra in rdo_letras:
            _rdo_res = work_dir / f"RDO_{letra}_{fin.strftime('%Y%m%d')}" / f"YUPANA_{fin.strftime('%d%m')}{letra}" / "RESULTADOS"
            df_rdo   = cargar_dataframe(_rdo_res, stem_rer)
            vals_rdo = rellenar_hasta_48(totales_rer(df_rdo, [x.upper() for x in barras_eol]))
            if vals_rdo: series_rer[f"RDO {letra}"] = vals_rdo

        if series_rer:
            fig, ax = plt.subplots(figsize=(11, 5))
            y_plot = []
            for nombre, valores in series_rer.items():
                xlab, yv = recortar_ceros_inicio(valores, _horas_e)
                if not yv: continue
                y_plot.extend(yv)
                ax.plot(xlab, yv, marker="o", linewidth=2, label=nombre)
            if y_plot:
                ax.set_ylim(max(0, math.floor(min(y_plot)) - 10), math.ceil(max(y_plot)) + 10)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_xticks(_ticks_pos_e); ax.set_xticklabels(_ticks_lbl_e, rotation=90, ha="right", fontsize=8)
            ax.set_title("EÓLICO")
            ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # ERROR EÓLICO (PDF)
        try:
            def _nz(x):
                try:
                    v = float(x) if x is not None else 0.0
                    return v if np.isfinite(v) else 0.0
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
                    if not np.isfinite(f): 
                        return np.nan
                    if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol:
                        return np.nan
                    return f
                except Exception:
                    return np.nan
        
            if series_rer:
                orden_series_eol = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_rer]
                pares_eol = [(orden_series_eol[i], orden_series_eol[i+1]) for i in range(len(orden_series_eol)-1)]
                errores_eol = {}
                for idx, (ante, act) in enumerate(pares_eol, start=1):
                    va, vb = series_rer[ante], series_rer[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0: 
                        continue
                    errores_eol[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                if errores_eol and any(len(v) > 0 for v in errores_eol.values()):
                    L = min(len(v) for v in errores_eol.values() if v)
                    x = np.arange(L)
                    start = datetime.strptime("00:30", "%H:%M")
                    labels = [(start + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(L)]
        
                    fig, ax = plt.subplots(figsize=(11, 5))
                    for k in sorted(errores_eol.keys(), key=lambda s: int(s.replace("error", ""))):
                        serie = [_omit_0_100(v) for v in errores_eol[k][:L]]
                        ax.plot(x, np.array(serie, dtype=float), marker='o', linewidth=2, label=k)
        
                    ax.set_title("Error Porcentual de EÓLICO")
                    ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=90, fontsize=8)
                    ax.legend(); plt.tight_layout()
                    pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass

        # HISTÓRICO EÓLICO (IEOD) — ini..fin
        series_ieod_eolica = {}
        cur = ini
        while cur <= fin:
            try:
                fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                vals = _extrae_eolica_48(fb)
                if vals and any(v != 0 for v in vals):
                    series_ieod_eolica[cur.strftime("%Y-%m-%d")] = vals[:48]
            except Exception:
                pass
            cur += timedelta(days=1)

        #if series_ieod_eolica:
            #fig, ax = plt.subplots(figsize=(12, 6))
            #x = list(range(48))
            #for lbl in sorted(series_ieod_eolica.keys()):
                #v = [0 if (vi is None or (isinstance(vi, float) and math.isnan(vi))) else vi
                    #for vi in series_ieod_eolica[lbl][:48]]
                #ax.plot(x, v, marker="o", linewidth=2, label=lbl)
            #ax.set_xticks(_ticks_pos_e); ax.set_xticklabels(_ticks_lbl_e, rotation=90, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO EÓLICO DE IEOD")
            #ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha")
            #plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # HISTÓRICO EÓLICO (RPO A) — ini..fin
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
                    r = requests.get(url_zip, timeout=40); r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                except Exception:
                    continue
            df_rer = cargar_dataframe(resultados, stem_rer)
            tot_eol = rellenar_hasta_48(totales_rer(df_rer, [x.upper() for x in barras_eol]))
            if tot_eol: series_eol_dia[f.strftime("%Y-%m-%d")] = tot_eol

        #if series_eol_dia:
            #fig, ax = plt.subplots(figsize=(11, 5))
            #y_all = []
            #for fecha_lbl, vals in series_eol_dia.items():
                #xlab, yv = recortar_ceros_inicio(vals, _horas_e)
                #if not yv: continue
                #y_all.extend(yv)
                #ax.plot(xlab, yv, marker="o", linewidth=2, label=fecha_lbl)
            #if y_all:
                #ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
            #ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            #ax.grid(axis="y", linestyle="--", alpha=0.5)
            #ax.set_xticks(_ticks_pos_e); ax.set_xticklabels(_ticks_lbl_e, rotation=90, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO EÓLICO DE RPO A")
            #ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
            #plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # FUSIÓN EÓLICA (IEOD ini..fin-1 + RDO-A fin)
        series_eolica_7 = {}
        cur = ini
        while cur < fin:
            lbl = cur.strftime("%Y-%m-%d")
            if lbl in series_ieod_eolica:
                series_eolica_7[lbl] = series_ieod_eolica[lbl][:48]
            else:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vals = _extrae_eolica_48(fb)
                    if vals: series_eolica_7[lbl] = vals[:48]
                except Exception:
                    pass
            cur += timedelta(days=1)
        lbl_fin = fin.strftime("%Y-%m-%d")
        if lbl_fin in series_eol_dia:
            series_eolica_7[lbl_fin] = series_eol_dia[lbl_fin][:48]

        if series_eolica_7:
            fechas_orden = []
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_eolica_7: fechas_orden.append(l)
                cur += timedelta(days=1)
            fig, ax = plt.subplots(figsize=(12, 6))
            x_idx = list(range(48)); y_all=[]
            for l in fechas_orden:
                fobj = datetime.strptime(l, "%Y-%m-%d").date()
                estilo = '--' if fobj < fin else '-'
                vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_eolica_7[l][:48]]
                y_all.extend(vals)
                ax.plot(x_idx, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
            ax.set_xticks(_ticks_pos_e); ax.set_xticklabels(_ticks_lbl_e, rotation=90, ha="right", fontsize=8)
            if y_all:
                ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_title("HISTÓRICO EÓLICO")
            ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Promedio diario
        if series_eolica_7:
            fechas_lbl=[]; promedios=[]
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_eolica_7:
                    vals = series_eolica_7[l][:48]
                    vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in vals]
                    promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                cur += timedelta(days=1)
            if promedios:
                fig, ax = plt.subplots(figsize=(9,5))
                bars = ax.bar(fechas_lbl, promedios)
                for rect, val in zip(bars, promedios):
                    ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                            ha="center", va="bottom", fontsize=9, clip_on=True)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
                ax.set_title("HISTÓRICO EÓLICO (Potencia Promedio Diario)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)

        # Promedio diario (Norte/Centro)
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

                v_n = pd.to_numeric(df.iloc[fila_rot+1:fila_rot+1+N_INTERVALOS, col_norte ], errors="coerce").fillna(0.0).astype(float).tolist()
                v_c = pd.to_numeric(df.iloc[fila_rot+1:fila_rot+1+N_INTERVALOS, col_centro], errors="coerce").fillna(0.0).astype(float).tolist()
                v_n = (v_n + [0.0]*N_INTERVALOS)[:N_INTERVALOS]
                v_c = (v_c + [0.0]*N_INTERVALOS)[:N_INTERVALOS]
                return v_n, v_c

            # IEOD: promedios diarios (ini → fin-1)
            fechas_nc, prom_norte, prom_centro = [], [], []
            cur = ini
            while cur < fin:
                try:
                    fb = _lee_ieod_bytes(cur.year, cur.month, MES_TXT[cur.month-1], cur.day)
                    vn, vc = _extrae_eolica_ns_gareas(fb)
                    if vn is not None and vc is not None:
                        fechas_nc.append(cur.strftime("%Y-%m-%d"))
                        prom_norte.append(sum(vn)/N_INTERVALOS)
                        prom_centro.append(sum(vc)/N_INTERVALOS)
                except Exception:
                    pass
                cur += timedelta(days=1)

            # Día fin: RDO-A dividido por NORTE/CENTRO
            try:
                f = fin
                yk, mk, dk = f.year, f.strftime("%m"), f.strftime("%d")
                M_TXT = MES_TXT[f.month-1]
                carpeta = work_dir / f"RDO_A_{yk}{mk}{dk}"
                resultados = carpeta / f"YUPANA_{dk}{mk}A" / "RESULTADOS"
                if not resultados.exists():
                    r = requests.get(base_rdo.format(y=yk, m=mk, d=dk, M=M_TXT, letra="A"), timeout=40); r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)

                df_rer = cargar_dataframe(resultados, stem_rer)
                NORTE  = {"PE TALARA","PE CUPISNIQUE","HUAMBOS","DUNA"}
                CENTRO = set(x.upper() for x in barras_eol) - NORTE

                vn = rellenar_hasta_48(totales_rer(df_rer, list(NORTE)))  or [0.0]*N_INTERVALOS
                vc = rellenar_hasta_48(totales_rer(df_rer, list(CENTRO))) or [0.0]*N_INTERVALOS

                fechas_nc.append(f.strftime("%Y-%m-%d"))
                prom_norte.append(sum(vn)/N_INTERVALOS)
                prom_centro.append(sum(vc)/N_INTERVALOS)
            except Exception:
                pass

            # Gráfico apilado
            if fechas_nc and prom_norte and prom_centro and len(fechas_nc)==len(prom_norte)==len(prom_centro):
                fig, ax = plt.subplots(figsize=(11, 5))
                ax.bar(fechas_nc, prom_norte, label="Norte")
                ax.bar(fechas_nc, prom_centro, bottom=prom_norte, label="Centro")
                for xlbl, a, b in zip(fechas_nc, prom_norte, prom_centro):
                    ax.text(xlbl, a+b+1, f"{a+b:.0f}", ha="center", va="bottom", fontsize=9)
                ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
                ax.set_title("HISTÓRICO EÓLICO (Potencia Promedio Diario) - Norte/Centro")
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                ax.legend()
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass
        
        # Máximo diario (Σ/2)
        if series_eolica_7:
            fechas_lbl=[]; maximos=[]
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_eolica_7:
                    vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v)
                            for v in series_eolica_7[l][:48]]
                    maximos.append(sum(vals)/2.0); fechas_lbl.append(l)
                cur += timedelta(days=1)
            if maximos:
                fig, ax = plt.subplots(figsize=(9,5))
                bars = ax.bar(fechas_lbl, maximos)
                for rect, val in zip(bars, maximos):
                    ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                            ha="center", va="bottom", fontsize=9, clip_on=True)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.set_xlabel("Fecha"); ax.set_ylabel("MWh")
                ax.set_title("HISTÓRICO EÓLICO (Energía Máxima Diaria)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
                
        # SOLAR (PDF)
        _inicio_s = datetime(2000, 1, 1, 0, 30)
        _horas_s  = [(_inicio_s + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(48)]
        _horas_s[-1] = "23:59"
        _ticks_pos_s = list(range(0, 48, 2))
        _ticks_lbl_s = [_horas_s[i] for i in _ticks_pos_s]
        
        fecha_str = fin.strftime("%Y%m%d")
        ddmm      = fin.strftime("%d%m")
        _pdo_res  = work_dir / f"PDO_{fecha_str}" / f"YUPANA_{fecha_str}" / "RESULTADOS"
        
        stem_rer = "Rer y No COES - Despacho (MW)"
        barras_solar = [
            "MAJES","REPARTICION","TACNASOLAR","PANAMERICANASOLAR","MOQUEGUASOLAR",
            "CS RUBI","INTIPAMPA","CSF YARUCAYA","CSCLEMESI","CS CARHUAQUERO",
            "CS MATARANI","CS SAN MARTIN","CS SUNNY"
        ]
        
        def _lee_ieod_bytes_s(y2, m2, d2):
            ddmm2 = f"{d2:02d}{m2:02d}"
            M2    = MES_TXT[m2-1]
            url   = base_ieod.format(y=y2, m=f"{m2:02d}", M=M2, d=f"{d2:02d}", ddmm=ddmm2)
            r = requests.get(url, timeout=40); r.raise_for_status()
            return io.BytesIO(r.content)
        
        def _extrae_solar_48_s(fb):
            df = pd.read_excel(fb, sheet_name="TIPO_RECURSO", header=5, engine="openpyxl")
            col = None
            for c in df.columns:
                if isinstance(c, str) and "SOLAR" in c.upper():
                    col = c; break
            if not col: return None
            vals = pd.to_numeric(df[col].iloc[:48], errors="coerce").fillna(0.0).astype(float).tolist()
            return (vals + [0.0]*48)[:48]
        
        # SOLAR actual (PDO vs RDO A–E)
        series_sol = {}
        df_pdo_sol = cargar_dataframe(_pdo_res, stem_rer)
        vals_pdo   = rellenar_hasta_48(totales_rer(df_pdo_sol, [x.upper() for x in barras_solar]))
        if vals_pdo: series_sol["PDO"] = vals_pdo
        
        for letra in rdo_letras:
            _rdo_res = work_dir / f"RDO_{letra}_{fecha_str}" / f"YUPANA_{ddmm}{letra}" / "RESULTADOS"
            df_rdo   = cargar_dataframe(_rdo_res, stem_rer)
            vals_rdo = rellenar_hasta_48(totales_rer(df_rdo, [x.upper() for x in barras_solar]))
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
                if all(v is None for v in y_vals): continue
                y_plot.extend([v for v in y_vals if v is not None])
                ax.plot(_horas_s, y_vals, marker="o", linewidth=2, label=nombre)
            if y_plot:
                ax.set_ylim(max(0, math.floor(min(y_plot)) - 10), math.ceil(max(y_plot)) + 10)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_xticks(_ticks_pos_s); ax.set_xticklabels(_ticks_lbl_s, rotation=90, ha="right", fontsize=8)
            ax.set_title("SOLAR"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend()
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        # ERROR SOLAR (PDF)
        try:
            import numpy as np
            from datetime import datetime, timedelta
        
            def _nz(x):
                try:
                    v = float(x) if x is not None else 0.0
                    return v if np.isfinite(v) else 0.0
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
                    if not np.isfinite(f):
                        return np.nan
                    if abs(f - 0.0) <= tol or abs(f - 100.0) <= tol:
                        return np.nan
                    return f
                except Exception:
                    return np.nan
        
            if series_sol:
                orden_series_sol = [k for k in (["PDO"] + [f"RDO {l}" for l in rdo_letras]) if k in series_sol]
                pares_sol = [(orden_series_sol[i], orden_series_sol[i+1]) for i in range(len(orden_series_sol)-1)]
                errores_sol = {}
                for idx, (ante, act) in enumerate(pares_sol, start=1):
                    va, vb = series_sol[ante], series_sol[act]
                    mL = min(len(va), len(vb), 48)
                    if mL <= 0:
                        continue
                    errores_sol[f"error{idx}"] = [_rel_err_abs_pct(va[i], vb[i]) for i in range(mL)]
        
                if errores_sol and any(len(v) > 0 for v in errores_sol.values()):
                    L = min(len(v) for v in errores_sol.values() if v)
                    x = np.arange(L)
                    start = datetime.strptime("00:30", "%H:%M")
                    labels = [(start + timedelta(minutes=30*i)).strftime("%H:%M") for i in range(L)]
        
                    fig, ax = plt.subplots(figsize=(11, 5))
                    for k in sorted(errores_sol.keys(), key=lambda s: int(s.replace("error", ""))):
                        serie = [_omit_0_100(v) for v in errores_sol[k][:L]]
                        ax.plot(x, np.array(serie, dtype=float), marker='o', linewidth=2, label=k)
        
                    ax.set_title("Error Porcentual de SOLAR")
                    ax.set_xlabel("Hora"); ax.set_ylabel("Error absoluto (%)")
                    ax.grid(axis="y", linestyle="--", alpha=0.5)
                    ax.set_xticks(x); ax.set_xticklabels(labels, rotation=90, fontsize=8)
                    ax.legend(); plt.tight_layout()
                    pdf.savefig(fig); plt.close(fig)
        except Exception:
            pass
        
        # HISTÓRICO SOLAR (IEOD) — ini..fin
        series_ieod_solar = {}
        cur = ini
        while cur <= fin:
            try:
                fb   = _lee_ieod_bytes_s(cur.year, cur.month, cur.day)
                vals = _extrae_solar_48_s(fb)
                if vals and any(v != 0 for v in vals):
                    series_ieod_solar[cur.strftime("%Y-%m-%d")] = vals[:48]
            except Exception:
                pass
            cur += timedelta(days=1)
        
        #if series_ieod_solar:
            #fig, ax = plt.subplots(figsize=(12, 6))
            #x = list(range(48))
            #for lbl in sorted(series_ieod_solar.keys()):
                #v = [0 if (vi is None or (isinstance(vi, float) and math.isnan(vi))) else vi
                     #for vi in series_ieod_solar[lbl][:48]]
                #ax.plot(x, v, marker="o", linewidth=2, label=lbl)
            #ax.set_xticks(_ticks_pos_s); ax.set_xticklabels(_ticks_lbl_s, rotation=90, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO SOLAR DE IEOD"); ax.set_xlabel("Hora"); ax.set_ylabel("MW")
            #ax.grid(axis="y", linestyle="--", alpha=0.4); ax.legend(title="Fecha")
            #plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        # HISTÓRICO SOLAR (RPO A) — ini..fin
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
                    r = requests.get(url_zip, timeout=40); r.raise_for_status()
                    with zipfile.ZipFile(io.BytesIO(r.content)) as zf: zf.extractall(path=carpeta)
                except Exception:
                    continue
            df_sol = cargar_dataframe(resultados, stem_rer)
            vals   = rellenar_hasta_48(totales_rer(df_sol, [x.upper() for x in barras_solar]))
            if vals and any(v != 0 for v in vals):
                series_sol_dia[f.strftime("%Y-%m-%d")] = vals
        
        #if series_sol_dia:
            #fig, ax = plt.subplots(figsize=(11, 5))
            #y_all = []
            #for fecha_lbl, raw_vals in series_sol_dia.items():
                #y_vals = []
                #for i, v in enumerate(raw_vals[:48]):
                    #v = 0 if pd.isna(v) else v
                    #if v == 0 and not (0 <= i <= 11 or 36 <= i <= 47):
                        #y_vals.append(None)
                    #else:
                        #y_vals.append(v)
                #if all(v is None for v in y_vals): continue
                #y_all.extend([v for v in y_vals if v is not None])
                #ax.plot(_horas_s, y_vals, marker="o", linewidth=2, label=fecha_lbl)
            #if y_all:
                #ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
            #ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            #ax.grid(axis="y", linestyle="--", alpha=0.5)
            #ax.set_xticks(_ticks_pos_s); ax.set_xticklabels(_ticks_lbl_s, rotation=90, ha="right", fontsize=8)
            #ax.set_title("HISTÓRICO SOLAR DE RPO A"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
            #plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        # FUSIÓN SOLAR (IEOD ini..fin-1 + RDO-A fin)
        series_solar_7 = {}
        cur = ini
        while cur < fin:
            lbl = cur.strftime("%Y-%m-%d")
            if lbl in series_ieod_solar:
                series_solar_7[lbl] = series_ieod_solar[lbl][:48]
            else:
                try:
                    fb   = _lee_ieod_bytes_s(cur.year, cur.month, cur.day)
                    vals = _extrae_solar_48_s(fb)
                    if vals: series_solar_7[lbl] = vals[:48]
                except Exception:
                    pass
            cur += timedelta(days=1)
        lbl_fin = fin.strftime("%Y-%m-%d")
        if lbl_fin in series_sol_dia:
            series_solar_7[lbl_fin] = series_sol_dia[lbl_fin][:48]
        
        if series_solar_7:
            fechas_orden = []
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_solar_7: fechas_orden.append(l)
                cur += timedelta(days=1)
            fig, ax = plt.subplots(figsize=(12, 6))
            x_idx = list(range(48)); y_all=[]
            for l in fechas_orden:
                fobj = datetime.strptime(l, "%Y-%m-%d").date()
                estilo = '--' if fobj < fin else '-'
                vals = [0 if (v is None or (isinstance(v,float) and math.isnan(v))) else v for v in series_solar_7[l][:48]]
                y_all.extend(vals)
                ax.plot(x_idx, vals, marker="o", linewidth=2, linestyle=estilo, label=l)
            ax.set_xticks(_ticks_pos_s); ax.set_xticklabels(_ticks_lbl_s, rotation=90, ha="right", fontsize=8)
            if y_all:
                ax.set_ylim(max(0, math.floor(min(y_all)) - 10), math.ceil(max(y_all)) + 10)
            ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
            ax.grid(axis="y", linestyle="--", alpha=0.5)
            ax.set_title("HISTÓRICO SOLAR"); ax.set_xlabel("Hora"); ax.set_ylabel("MW"); ax.legend(title="Fecha")
            plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        # Promedio diario
        if series_solar_7:
            fechas_lbl=[]; promedios=[]
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_solar_7:
                    vals = series_solar_7[l][:48]
                    vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v) for v in vals]
                    promedios.append(sum(vals)/48.0); fechas_lbl.append(l)
                cur += timedelta(days=1)
            if promedios:
                fig, ax = plt.subplots(figsize=(9,5))
                bars = ax.bar(fechas_lbl, promedios)
                for rect, val in zip(bars, promedios):
                    ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                            ha="center", va="bottom", fontsize=9, clip_on=True)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.set_xlabel("Fecha"); ax.set_ylabel("MW")
                ax.set_title("HISTÓRICO SOLAR (Potencia Promedio Diario)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
        
        # Máximo diario (Σ/2)
        if series_solar_7:
            fechas_lbl=[]; maximos=[]
            cur = ini
            while cur <= fin:
                l = cur.strftime("%Y-%m-%d")
                if l in series_solar_7:
                    vals = [0.0 if (v is None or (isinstance(v,float) and math.isnan(v))) else float(v)
                            for v in series_solar_7[l][:48]]
                    maximos.append(sum(vals)/2.0); fechas_lbl.append(l)
                cur += timedelta(days=1)
            if maximos:
                fig, ax = plt.subplots(figsize=(9,5))
                bars = ax.bar(fechas_lbl, maximos)
                for rect, val in zip(bars, maximos):
                    ax.text(rect.get_x()+rect.get_width()/2, rect.get_height(), f"{val:.0f}",
                            ha="center", va="bottom", fontsize=9, clip_on=True)
                ax.yaxis.set_major_locator(mticker.MaxNLocator(integer=True))
                ax.set_xlabel("Fecha"); ax.set_ylabel("MWh")
                ax.set_title("HISTÓRICO SOLAR (Energía Máxima Diaria)")
                ax.grid(axis="y", linestyle="--", alpha=0.4)
                plt.tight_layout(); pdf.savefig(fig); plt.close(fig)
                
        render_graficos_a_pdf(ini=ini, fin=fin, barras=barras, rdo_letras=rdo_letras, work_dir=work_dir, pdf=pdf)
        pdf.close()

    try:
        pdf_bytes = (work_dir / "Reporte.pdf").read_bytes()
    except Exception:
        pass
        
st.caption("© Reporte Programa Diario de Operación - USGE")
