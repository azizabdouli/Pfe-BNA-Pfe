# app.py

# ==== 📦 Imports ====
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
import plotly.io as pio
from io import BytesIO
from reportlab.lib.pagesizes import A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import cm
from reportlab.lib import colors
import zipfile
import datetime as _dt
import inspect
from typing import Optional

from modules.data_mapper import prepare_ppna_data

# ==== ⚙️ Configuration ====
pio.templates.default = "plotly_white"
st.set_page_config(page_title="IFRS 17 - Outil PAA", layout="wide")

# ==== 🧾 Titre ====
st.title("🔍 Outil IFRS 17 - Approche PAA")

# -----------------------------------------------------------------------------
#                Streamlit width compatibility (deprecation-proof)
# -----------------------------------------------------------------------------
def _supports_width_kw(func) -> bool:
    return "width" in inspect.signature(func).parameters

def df_full(df, **kwargs):
    """Full-width dataframe, future-proof."""
    if _supports_width_kw(st.dataframe):
        return st.dataframe(df, width="stretch", **kwargs)
    else:
        return st.dataframe(df, use_container_width=True, **kwargs)

def editor_full(df, **kwargs):
    """Full-width data editor, future-proof."""
    if _supports_width_kw(st.data_editor):
        return st.data_editor(df, width="stretch", **kwargs)
    else:
        return st.data_editor(df, use_container_width=True, **kwargs)

def plot_full(fig, **kwargs):
    """Full-width Plotly chart, future-proof."""
    if _supports_width_kw(st.plotly_chart):
        return st.plotly_chart(fig, width="stretch", **kwargs)
    else:
        return st.plotly_chart(fig, use_container_width=True, **kwargs)

# -----------------------------------------------------------------------------
#                               Helpers génériques
# -----------------------------------------------------------------------------
def to_datetime_safe(s, fmt=None):
    if fmt:
        return pd.to_datetime(s, format=fmt, errors='coerce')
    return pd.to_datetime(s, errors='coerce')

def cap_date(date_series, years_forward=10):
    ser = pd.to_datetime(date_series, errors='coerce')
    cap = pd.Timestamp.today() + pd.DateOffset(years=years_forward)
    ser = ser.where((ser.isna()) | (ser <= cap), cap)
    return ser

def normalise_pattern(row):
    """Normalise le pattern 12 mois pour qu'il somme à 1 (fallback uniforme)."""
    cols = [f"M{i}" for i in range(1, 13)]
    vals = pd.to_numeric(row[cols], errors="coerce").fillna(0.0).values.astype(float)
    s = vals.sum()
    if s <= 0:
        return np.array([1/12.0]*12)
    return vals / s

# -----------------------------------------------------------------------------
#                   Projection mensuelle EXACTE (avec pattern)
# -----------------------------------------------------------------------------
def monthly_projection_exact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Projection mensuelle EXACTE par contrat :
    - génère 'duree_mois' périodes MS à partir du mois d'effet (début de mois),
    - répartit la prime lissée selon pattern 12 mois si disponible (sinon uniforme),
    - amortit DAC au même pattern (ici uniforme par défaut),
    - retourne colonnes: [ID (si dispo), mois, revenue_mois, dac_amort_mois, CODPROD, Cohorte, Onereux]
    """
    if len(df) == 0:
        return pd.DataFrame(columns=["mois", "revenue_mois", "dac_amort_mois"])

    work = df.copy()

    # Bases
    work['date_effet'] = to_datetime_safe(work['date_effet'])
    work['duree_mois'] = pd.to_numeric(work['duree_mois'], errors='coerce').fillna(0).astype(int)
    work = work[work['duree_mois'] > 0].copy()

    # Identifiant contrat si dispo
    id_col = None
    for c in ['NUMQUITT', 'NUMCONTRAT', 'ID_CONTRAT']:
        if c in work.columns:
            id_col = c
            break

    # Cohorte & groupe onéreux
    work['Cohorte'] = work['date_effet'].dt.year
    work['Onereux'] = work['lrc'] < 0 if 'lrc' in work else False

    # Pattern
    pat_cols = [f"M{i}" for i in range(1, 13)]
    for c in pat_cols:
        if c not in work.columns:
            work[c] = np.nan
    pattern_arr = work.apply(normalise_pattern, axis=1)

    # Prime >= 0 (reconnaissance de service)
    prime_pos = work['prime_brute'].clip(lower=0).fillna(0.0).values.astype(float)
    # DAC (si absente, calcule via DAC_pct si fourni)
    if 'dac' not in work.columns:
        if 'DAC_pct' in work.columns:
            work['dac'] = prime_pos * pd.to_numeric(work['DAC_pct'], errors='coerce').fillna(0.10)
        else:
            work['dac'] = prime_pos * 0.10

    rows = []
    for idx, r in work.iterrows():
        start_m = r['date_effet'].to_period('M').to_timestamp()  # début du mois
        n = int(r['duree_mois'])
        if n <= 0 or pd.isna(start_m):
            continue

        pat12 = pattern_arr.iloc[idx]
        if n <= 12:
            pat = pat12[:n]
        else:
            k = n // 12
            rem = n % 12
            pat = np.concatenate([np.tile(pat12, k), pat12[:rem]])
        pat = pat / pat.sum()  # re-normalise

        rev_mois = (prime_pos[idx] * pat).astype(float)
        dac_amort = (float(r['dac']) * (np.ones(n)/n)).astype(float)  # DAC amortie uniforme

        months = pd.date_range(start=start_m, periods=n, freq='MS')
        for m, rv, da in zip(months, rev_mois, dac_amort):
            out = {
                'mois': m,
                'revenue_mois': rv,
                'dac_amort_mois': da,
                'CODPROD': r.get('CODPROD', None),
                'Cohorte': r.get('Cohorte', None),
                'Onereux': r.get('Onereux', False)
            }
            if id_col:
                out[id_col] = r[id_col]
            rows.append(out)

    proj = pd.DataFrame(rows)
    return proj

# -----------------------------------------------------------------------------
#                       Exports (gros volumes gérés)
# -----------------------------------------------------------------------------
MAX_XLSX_ROWS = 1_000_000  # sécurité sous la limite Excel (1 048 576)

def _write_df_chunked(writer, df: pd.DataFrame, base_sheet_name: str):
    n = len(df)
    if n == 0:
        df.head(0).to_excel(writer, index=False, sheet_name=f"{base_sheet_name}_empty")
        return [f"{base_sheet_name}_empty"]
    names = []
    start = 0
    part = 1
    while start < n:
        end = min(start + MAX_XLSX_ROWS, n)
        sheet = f"{base_sheet_name}_{part}"
        df.iloc[start:end].to_excel(writer, index=False, sheet_name=sheet)
        names.append(sheet)
        start = end
        part += 1
    return names

def export_excel_chunked(df_main: pd.DataFrame, df_rev_agg: pd.DataFrame, df_proj: pd.DataFrame) -> bytes:
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='yyyy-mm-dd') as writer:
        sheets_main = _write_df_chunked(writer, df_main, "IFRS17_PAA_Data")
        sheets_rev  = _write_df_chunked(writer, df_rev_agg, "Revenue_Mensuel_Agg")
        sheets_proj = _write_df_chunked(writer, df_proj, "Projection_Mensuelle")
        for sheet in [sheets_main[0], sheets_rev[0], sheets_proj[0]]:
            ws = writer.sheets[sheet]
            ws.set_column(0, 20, 18)
    return output.getvalue()

def export_zip_csv(df_dict: dict) -> bytes:
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, df in df_dict.items():
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            zf.writestr(f"{name}.csv", csv_bytes)
    mem_zip.seek(0)
    return mem_zip.getvalue()

def export_pdf_summary(df_main: pd.DataFrame, df_rev_agg: pd.DataFrame) -> bytes:
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    x0, y0 = 2*cm, height - 2*cm
    c.setFont("Helvetica-Bold", 16)
    c.drawString(x0, y0, "IFRS 17 – Approche PAA : Rapport de synthèse")
    y = y0 - 1.0*cm

    total_contracts = int(len(df_main))
    total_premium = float(df_main.get('prime_brute', pd.Series(dtype=float)).clip(lower=0).sum())
    onerous_pct = float(100 * (df_main.get('lrc', pd.Series(dtype=float)) < 0).mean()) if len(df_main) else 0.0

    c.setFont("Helvetica", 11)
    c.drawString(x0, y, f"📦 Contrats (filtrés) : {total_contracts:,}")
    y -= 0.6*cm
    c.drawString(x0, y, f"💰 Prime brute totale (TND) : {total_premium:,.0f}")
    y -= 0.6*cm
    c.drawString(x0, y, f"⚠️ % LRC négative : {onerous_pct:.2f}%")
    y -= 1.0*cm

    c.setFont("Helvetica-Bold", 12)
    c.drawString(x0, y, "Revenus IFRS 17 – Agrégation mensuelle (aperçu)")
    y -= 0.7*cm

    c.setFont("Helvetica", 10)
    c.drawString(x0, y, "Mois")
    c.drawString(x0 + 7*cm, y, "Revenue (TND)")
    y -= 0.4*cm
    c.setLineWidth(0.5)
    c.setStrokeColor(colors.black)
    c.line(x0, y, x0 + 12*cm, y)
    y -= 0.3*cm

    prev = df_rev_agg.head(12).copy()
    for _, r in prev.iterrows():
        mois = pd.to_datetime(r['mois']).strftime('%Y-%m') if pd.notna(r['mois']) else ''
        rev = f"{float(r['revenue_plot']):,.0f}"
        c.drawString(x0, y, mois); c.drawString(x0 + 7*cm, y, rev); y -= 0.45*cm
        if y < 2.5*cm:
            c.showPage(); y = height - 2*cm
            c.setFont("Helvetica-Bold", 12); c.drawString(x0, y, "Revenus IFRS 17 – Suite")
            y -= 0.7*cm; c.setFont("Helvetica", 10)

    c.showPage(); c.save()
    pdf_bytes = buf.getvalue(); buf.close()
    return pdf_bytes

# -----------------------------------------------------------------------------
#                                 DATA UPLOAD
# -----------------------------------------------------------------------------
with st.expander("📂 Importer le fichier PPNA (.csv ou .xlsx)", expanded=True):
    uploaded_file = st.file_uploader("Fichier PPNA", type=["csv", "xlsx"], label_visibility="collapsed")

if not uploaded_file:
    st.info("🕒 Veuillez importer un fichier PPNA pour commencer.")
    st.stop()

# Lecture fichier
if uploaded_file.name.endswith(".csv"):
    df_raw = pd.read_csv(uploaded_file)
else:
    df_raw = pd.read_excel(uploaded_file)

st.success("✅ Données chargées avec succès")
st.caption("Aperçu des données brutes :")
df_full(df_raw.head())

# -----------------------------------------------------------------------------
#                        Transformation IFRS 17 (PAA)
# -----------------------------------------------------------------------------
df = prepare_ppna_data(df_raw)

# Nettoyage/typage
df['date_effet'] = to_datetime_safe(df.get('date_effet'))
df['date_fin']   = to_datetime_safe(df.get('date_fin'))
df = df.dropna(subset=['date_effet', 'date_fin']).copy()
df['duree_mois'] = pd.to_numeric(df.get('duree_mois'), errors='coerce')
df = df[df['duree_mois'].between(1, 120, inclusive='both')].copy()  # 1..120 mois
df['date_fin'] = cap_date(df['date_fin'], years_forward=10)

# Réassurance si dispo
if 'prime_cedee' in df.columns:
    df['prime_nette'] = df['prime_brute'].fillna(0) - df['prime_cedee'].fillna(0)

# Revenue proxy (uniforme linéaire par défaut)
df['revenue_plot'] = (df['prime_brute'].clip(lower=0) / df['duree_mois']).fillna(0)

# -----------------------------------------------------------------------------
#                              Onglets de navigation
# -----------------------------------------------------------------------------
tab_data, tab_params, tab_analytics, tab_proj, tab_exports, tab_log = st.tabs(
    ["📁 Données", "⚙️ Paramètres", "📊 Analyses", "🧮 Projection", "⬇️ Exports", "📒 Registre"]
)

# -----------------------------------------------------------------------------
#                                   📁 Données
# -----------------------------------------------------------------------------
with tab_data:
    st.subheader("Étape 1 - Transformation des données selon IFRS 17 (PAA)")
    cols_show = [
        'date_effet','date_fin','duree_mois','prime_brute','prime_acquise',
        'ppna_ifrs17','ppna_initiale','dac','lrc','revenue_mensuel','revenue_plot','CODPROD'
    ]
    df_full(df[[c for c in cols_show if c in df.columns]].head(10))

# -----------------------------------------------------------------------------
#                               ⚙️ Paramètres par produit
# -----------------------------------------------------------------------------
with tab_params:
    st.subheader("⚙️ Paramétrage par produit (DAC %, pattern de service 12 mois, éligibilité PAA > 12m)")

    codprod_series = df.get('CODPROD', pd.Series(dtype=object)).dropna().unique()
    default_params = pd.DataFrame({
        "CODPROD": sorted(codprod_series.tolist()) if len(codprod_series) else []
    })
    default_params["DAC_pct"] = 0.10
    default_params["Eligible_PAA"] = True
    for i in range(1, 13):
        default_params[f"M{i}"] = round(1/12, 6)

    c1, c2 = st.columns([2,1])
    with c1:
        st.caption("✏️ Modifie ici les paramètres (tu peux ajouter/supprimer des lignes).")
        params = editor_full(
            default_params,
            num_rows="dynamic",
            key="params_editor",
            column_config={f"M{i}": st.column_config.NumberColumn(format="%.6f") for i in range(1, 13)}
        )
    with c2:
        st.caption("📥/📤 Import/Export des paramètres")
        # Export
        params_csv = params.to_csv(index=False).encode("utf-8")
        st.download_button("📤 Exporter paramètres (CSV)", data=params_csv, file_name="IFRS17_Params_Produits.csv", mime="text/csv")
        # Import
        up_params = st.file_uploader("Importer paramètres (CSV)", type=["csv"], key="up_params")
        if up_params is not None:
            try:
                params = pd.read_csv(up_params)
                st.success("Paramètres importés. Ils seront utilisés ci-dessous.")
            except Exception as e:
                st.error(f"Import paramètres impossible : {e}")

    # Merge avec données principales
    if "CODPROD" in df.columns and len(params):
        df = df.merge(params, on="CODPROD", how="left")
        # Si dac absent, calcule à partir de DAC_pct
        if 'dac' not in df.columns:
            df['dac'] = (df['prime_brute'].clip(lower=0) * pd.to_numeric(df['DAC_pct'], errors='coerce').fillna(0.10)).fillna(0.0)

    st.info("✅ Paramètres appliqués. Ils seront pris en compte dans la projection exacte et la vue Groupe.")

# -----------------------------------------------------------------------------
#                                  🎛️ Filtres
# -----------------------------------------------------------------------------
with st.sidebar:
    st.header("🎛️ Filtres")
    years = df['date_effet'].dt.year.dropna()
    min_year = int(years.min()) if len(years) else 2000
    max_year = int(years.max()) if len(years) else pd.Timestamp.today().year
    year_range = st.slider("Années d'effet (affichage)", min_year, max_year, (min_year, max_year))
    df = df[(df['date_effet'].dt.year >= year_range[0]) & (df['date_effet'].dt.year <= year_range[1])]

    if 'CODPROD' in df.columns:
        prods = st.multiselect("Produit (CODPROD)", sorted(df['CODPROD'].dropna().unique().tolist()))
        if prods:
            df = df[df['CODPROD'].isin(prods)]

# -----------------------------------------------------------------------------
#                                    📊 Analyses
# -----------------------------------------------------------------------------
with tab_analytics:
    st.subheader("📊 PPNA comptable vs PPNA IFRS 17 (pondération par prime)")
    df_scatter = df[(df['prime_brute'] > 0) & df['ppna_initiale'].notna() & df['ppna_ifrs17'].notna()].copy()
    fig_ppna = px.scatter(
        df_scatter,
        x='ppna_initiale', y='ppna_ifrs17',
        color='lrc' if 'lrc' in df_scatter else None,
        size='prime_brute',
        hover_data={'date_effet': True, 'date_fin': True, 'prime_brute': ':.2f',
                    'ppna_ifrs17': ':.2f', 'ppna_initiale': ':.2f',
                    'lrc': ':.2f' if 'lrc' in df_scatter else False},
        color_continuous_scale='Viridis'
    )
    if len(df_scatter) > 0:
        fig_ppna.add_shape(
            type='line',
            x0=df_scatter['ppna_initiale'].min(), y0=df_scatter['ppna_initiale'].min(),
            x1=df_scatter['ppna_initiale'].max(), y1=df_scatter['ppna_initiale'].max(),
            line=dict(color="red", dash="dash"),
        )
    fig_ppna.update_layout(height=560)
    plot_full(fig_ppna)

    st.markdown("---")
    st.subheader("📈 Distribution des LRC")
    lrc_min = float(df['lrc'].quantile(0.01)) if 'lrc' in df and len(df) else -1000.0
    lrc_max = float(df['lrc'].quantile(0.99)) if 'lrc' in df and len(df) else 1000.0
    lrc_range = st.slider("Plage LRC (pour lisibilité)", lrc_min, lrc_max, (lrc_min, lrc_max))
    hist_percent = st.checkbox("Afficher en pourcentage", value=True, key="histpct")
    df_hist = df[(df.get('lrc', pd.Series(dtype=float)) >= lrc_range[0]) & (df.get('lrc', pd.Series(dtype=float)) <= lrc_range[1])]
    fig_lrc = go.Figure()
    fig_lrc.add_trace(go.Histogram(
        x=df_hist['lrc'] if 'lrc' in df_hist else [],
        nbinsx=60, marker_color='#636EFA', name="LRC calculées",
        histnorm="percent" if hist_percent else None
    ))
    fig_lrc.add_vline(x=0, line_width=2, line_dash="dash", line_color="red")
    fig_lrc.update_layout(title="Distribution des LRC – ligne rouge : 0",
                          xaxis_title="LRC",
                          yaxis_title="Pourcentage" if hist_percent else "Nombre",
                          height=460)
    plot_full(fig_lrc)

    st.markdown("---")
    st.subheader("📆 Revenus IFRS 17 reconnus – agrégation par mois d'effet (proxy)")
    df['mois_effet'] = df['date_effet'].dt.to_period('M')
    df_rev = df.groupby('mois_effet', as_index=False)['revenue_plot'].sum()
    if len(df_rev):
        df_rev['mois_effet'] = df_rev['mois_effet'].dt.to_timestamp()
    fig_revenue = px.bar(df_rev, x='mois_effet', y='revenue_plot',
                         title="Évolution mensuelle (revenus IFRS 17 – proxy lissé)",
                         labels={'mois_effet': "Mois", 'revenue_plot': "Revenue IFRS 17 (TND)"},
                         height=460)
    plot_full(fig_revenue)

    st.markdown("---")
    st.subheader("🏷️ Vue Groupe IFRS-17 (Portfolio × Cohorte × Onéreux)")
    df['Cohorte'] = df['date_effet'].dt.year
    df['Onereux'] = df.get('lrc', pd.Series(dtype=float)) < 0
    group_cols = ['CODPROD', 'Cohorte', 'Onereux']
    df_group = df.groupby(group_cols, dropna=False).agg(
        LRC_total=('lrc', 'sum'),
        DAC_total=('dac', 'sum'),
        Revenue_total=('revenue_plot', 'sum'),
        Contracts=('date_effet', 'count')
    ).reset_index()

    for _, r in df_group.iterrows():
        st.metric(
            label=f"{r['CODPROD']} – Cohorte {int(r['Cohorte'])} – {'Onéreux' if r['Onereux'] else 'Non-onéreux'}",
            value=f"LRC: {r['LRC_total']:,.0f} TND",
            delta=f"Contrats: {int(r['Contracts'])} | Revenue: {r['Revenue_total']:,.0f}"
        )

    df_full(df_group)

# -----------------------------------------------------------------------------
#                                🧮 Projection exacte
# -----------------------------------------------------------------------------
with tab_proj:
    st.subheader("🧮 Projection mensuelle EXACTE (par contrat)")
    st.caption("Utilise les patterns saisis (onglet Paramètres). DAC amortie uniforme par défaut.")

    yr_min_all = int(df['date_effet'].dt.year.min())
    yr_max_all = int(df['date_effet'].dt.year.max())
    years_export = st.slider("Années d'effet à inclure (projection)", yr_min_all, yr_max_all, (yr_min_all, yr_max_all), key="proj_years")
    df_export_proj = df[(df['date_effet'].dt.year >= years_export[0]) & (df['date_effet'].dt.year <= years_export[1])].copy()

    df_proj = monthly_projection_exact(df_export_proj)
    df_full(df_proj.head(10))

    st.markdown("##### 📈 Roll-forward agrégé par Groupe (somme mensuelle revenue & amort DAC)")
    if len(df_proj):
        rf = df_proj.groupby(['CODPROD','Cohorte','Onereux','mois'], dropna=False).agg(
            revenue=('revenue_mois','sum'),
            dac_amort=('dac_amort_mois','sum')
        ).reset_index()
        df_full(rf.head(20))
        st.download_button("⬇️ Exporter Roll-forward agrégé (CSV)",
                           data=rf.to_csv(index=False).encode("utf-8"),
                           file_name="IFRS17_Rollforward_Groupe.csv",
                           mime="text/csv")
    else:
        st.info("Pas de lignes dans la projection pour la plage sélectionnée.")

# -----------------------------------------------------------------------------
#                                    ⬇️ Exports
# -----------------------------------------------------------------------------
with tab_exports:
    st.subheader("⬇️ Exports professionnels")

    df_rev_agg = df_rev.rename(columns={'mois_effet': 'mois'})

    try:
        xlsx_bytes = export_excel_chunked(df_main=df, df_rev_agg=df_rev_agg, df_proj=df_proj if 'df_proj' in locals() else pd.DataFrame())
        st.download_button("📥 Télécharger Excel (multi-onglets) – IFRS17_PAA_Export.xlsx",
                           data=xlsx_bytes,
                           file_name="IFRS17_PAA_Export.xlsx",
                           mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    except Exception as ex:
        st.warning(f"Excel volumineux : {ex}")

    zip_bytes = export_zip_csv({
        "IFRS17_PAA_Data": df,
        "Revenue_Mensuel_Agg": df_rev_agg,
        "Projection_Mensuelle": df_proj if 'df_proj' in locals() else pd.DataFrame(),
    })
    st.download_button("🗜️ Télécharger ZIP (CSV) – Grand volume",
                       data=zip_bytes, file_name="IFRS17_PAA_Export.zip", mime="application/zip")

    pdf_bytes = export_pdf_summary(df_main=df, df_rev_agg=df_rev_agg)
    st.download_button("📄 Télécharger PDF (Rapport_Synthese_IFRS17_PAA.pdf)",
                       data=pdf_bytes, file_name="Rapport_Synthese_IFRS17_PAA.pdf", mime="application/pdf")

    st.success("✅ Exports prêts.")

# -----------------------------------------------------------------------------
#                                  📒 Registre
# -----------------------------------------------------------------------------
with tab_log:
    st.subheader("📒 Registre des Assumptions & Contexte")
    registre = {
        "Date_gel": [_dt.datetime.now().strftime("%Y-%m-%d %H:%M")],
        "Source_donnees": [uploaded_file.name],
        "Nb_contrats": [len(df)],
        "Nb_produits": [df.get('CODPROD', pd.Series(dtype=object)).nunique()],
        "DAC_moyen": [pd.to_numeric(df.get('DAC_pct', pd.Series([np.nan]*len(df))), errors='coerce').mean()],
        "Hypothese_pattern": ["Pattern saisi par produit (onglet Paramètres), uniforme si vide"],
        "Eligibilite_PAA_>12m": ["Flag 'Eligible_PAA' dans paramètres (documentation requise si True)"]
    }
    journal = pd.DataFrame(registre)
    df_full(journal)
    st.download_button("⬇️ Exporter registre (CSV)", data=journal.to_csv(index=False).encode("utf-8"),
                       file_name="IFRS17_Assumptions_Register.csv", mime="text/csv")
