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

from modules.data_mapper import prepare_ppna_data

# ==== ⚙️ Configuration ====
pio.templates.default = "plotly_white"
st.set_page_config(page_title="IFRS 17 - Outil PAA (PAA)", layout="wide")

# ==== 🧾 Titre ====
st.title("🔍 Outil IFRS 17 - Approche PAA")
st.subheader("Étape 1 - Chargement et traitement des données PPNA")

# ==== 📂 Upload ====
uploaded_file = st.file_uploader("📂 Importer le fichier PPNA (.csv ou .xlsx)", type=["csv", "xlsx"])

# ======= Helpers =======
def to_datetime_safe(s, fmt=None):
    if fmt:
        return pd.to_datetime(s, format=fmt, errors='coerce')
    return pd.to_datetime(s, errors='coerce')

def cap_date(date_series, years_forward=10):
    ser = pd.to_datetime(date_series, errors='coerce')
    cap = pd.Timestamp.today() + pd.DateOffset(years=years_forward)
    ser = ser.where((ser.isna()) | (ser <= cap), cap)
    return ser

def monthly_projection_exact(df: pd.DataFrame) -> pd.DataFrame:
    """
    Projection mensuelle EXACTE par contrat :
    - Génère 'duree_mois' périodes mensuelles à partir du MOIS d'effet.
    - Alloue 'revenue_plot' (prime brute lissée, non négative) à chacun des mois.
    """
    work = df.copy()

    for col in ['date_effet', 'duree_mois', 'prime_brute', 'revenue_plot']:
        if col not in work.columns:
            work[col] = np.nan

    work['date_effet'] = to_datetime_safe(work['date_effet'])
    work['duree_mois'] = pd.to_numeric(work['duree_mois'], errors='coerce').fillna(0).astype(int)
    work = work[work['duree_mois'] > 0].copy()

    start_month = work['date_effet'].dt.to_period('M').dt.to_timestamp()

    work['mois_list'] = [
        pd.date_range(start=s, periods=int(n), freq='MS') if pd.notna(s) and n > 0 else pd.DatetimeIndex([])
        for s, n in zip(start_month, work['duree_mois'])
    ]

    work['revenue_mois_list'] = [
        np.repeat(r, int(n)) if pd.notna(r) and n > 0 else np.array([])
        for r, n in zip(work['revenue_plot'], work['duree_mois'])
    ]

    id_col = None
    for c in ['NUMQUITT', 'NUMCONTRAT', 'ID_CONTRAT']:
        if c in work.columns:
            id_col = c
            break

    proj = work[[id_col] if id_col else []].copy()
    proj['mois'] = work['mois_list']
    proj['revenue_mois'] = work['revenue_mois_list']

    proj = proj.explode(['mois', 'revenue_mois'], ignore_index=True)
    proj['mois'] = to_datetime_safe(proj['mois'])
    proj['revenue_mois'] = pd.to_numeric(proj['revenue_mois'], errors='coerce').fillna(0.0)

    keep_cols = [c for c in [id_col, 'mois', 'revenue_mois'] if c is not None]
    return proj[keep_cols]

# ==== Exports (gros volumes gérés) ====
MAX_XLSX_ROWS = 1_000_000  # légèrement sous la limite Excel

def _write_df_chunked(writer, df: pd.DataFrame, base_sheet_name: str):
    """Écrit un DF sur plusieurs onglets si > MAX_XLSX_ROWS lignes."""
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
    """
    Export Excel multi-onglets avec découpage automatique si > 1 000 000 lignes.
    """
    output = BytesIO()
    with pd.ExcelWriter(output, engine='xlsxwriter', datetime_format='yyyy-mm-dd') as writer:
        sheets_main = _write_df_chunked(writer, df_main, "IFRS17_PAA_Data")
        sheets_rev  = _write_df_chunked(writer, df_rev_agg, "Revenue_Mensuel_Agg")
        sheets_proj = _write_df_chunked(writer, df_proj, "Projection_Mensuelle")

        # Ajuster largeur colonnes sur le 1er onglet de chaque bloc
        for sheet in [sheets_main[0], sheets_rev[0], sheets_proj[0]]:
            ws = writer.sheets[sheet]
            ws.set_column(0, 12, 18)
    return output.getvalue()

def export_zip_csv(df_dict: dict) -> bytes:
    """
    ZIP en mémoire avec un CSV par DataFrame (pas de limite de lignes comme XLSX).
    """
    mem_zip = BytesIO()
    with zipfile.ZipFile(mem_zip, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        for name, df in df_dict.items():
            csv_bytes = df.to_csv(index=False).encode("utf-8")
            zf.writestr(f"{name}.csv", csv_bytes)
    mem_zip.seek(0)
    return mem_zip.getvalue()

def export_pdf_summary(df_main: pd.DataFrame, df_rev_agg: pd.DataFrame) -> bytes:
    """PDF simple & pro : Titre, KPIs, aperçu agrégation mensuelle."""
    buf = BytesIO()
    c = canvas.Canvas(buf, pagesize=A4)
    width, height = A4

    x0, y0 = 2*cm, height - 2*cm
    c.setFont("Helvetica-Bold", 16)
    c.drawString(x0, y0, "IFRS 17 – Approche PAA : Rapport de synthèse")
    y = y0 - 1.0*cm

    total_contracts = int(len(df_main))
    total_premium = float(df_main['prime_brute'].clip(lower=0).sum()) if 'prime_brute' in df_main else 0.0
    onerous_pct = float(100 * (df_main['lrc'] < 0).mean()) if 'lrc' in df_main and len(df_main) else 0.0

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

    preview = df_rev_agg.head(12).copy()
    for _, r in preview.iterrows():
        mois = pd.to_datetime(r['mois']).strftime('%Y-%m') if pd.notna(r['mois']) else ''
        rev = f"{float(r['revenue_plot']):,.0f}"
        c.drawString(x0, y, mois)
        c.drawString(x0 + 7*cm, y, rev)
        y -= 0.45*cm
        if y < 2.5*cm:
            c.showPage()
            y = height - 2*cm
            c.setFont("Helvetica-Bold", 12)
            c.drawString(x0, y, "Revenus IFRS 17 – Suite")
            y -= 0.7*cm
            c.setFont("Helvetica", 10)

    c.showPage()
    c.save()
    pdf_bytes = buf.getvalue()
    buf.close()
    return pdf_bytes

# ======= App flow =======
if uploaded_file:
    try:
        # === Lecture fichier ===
        if uploaded_file.name.endswith(".csv"):
            df_raw = pd.read_csv(uploaded_file)
        else:
            df_raw = pd.read_excel(uploaded_file)

        st.success("✅ Données chargées avec succès !")
        st.write("🔍 Aperçu des données brutes :")
        st.dataframe(df_raw.head())

        # === Transformation IFRS 17 (PAA) ===
        st.subheader("🔄 Transformation des données selon IFRS 17 (Approche PAA)")
        df = prepare_ppna_data(df_raw)

        # Sécuriser types et nettoyage
        df['date_effet'] = to_datetime_safe(df.get('date_effet'))
        df['date_fin']   = to_datetime_safe(df.get('date_fin'))
        df = df.dropna(subset=['date_effet', 'date_fin']).copy()

        df['duree_mois'] = pd.to_numeric(df.get('duree_mois'), errors='coerce')
        df = df[df['duree_mois'].between(1, 120, inclusive='both')]  # 1..120 mois
        df['date_fin'] = cap_date(df['date_fin'], years_forward=10)

        # Revenu pour traçage (lissé, non négatif)
        df['revenue_plot'] = (df['prime_brute'].clip(lower=0) / df['duree_mois']).fillna(0)

        # Aperçu
        st.write("📋 Données enrichies avec calculs IFRS 17 :")
        cols_show = [
            'date_effet','date_fin','duree_mois','prime_brute','prime_acquise',
            'ppna_ifrs17','ppna_initiale','dac','lrc','revenue_mensuel','revenue_plot'
        ]
        st.dataframe(df[[c for c in cols_show if c in df.columns]].head(10))

        # KPIs
        total_contracts = int(len(df))
        total_premium = float(df['prime_brute'].clip(lower=0).sum()) if 'prime_brute' in df else 0.0
        onerous_pct = float(100 * (df['lrc'] < 0).mean()) if 'lrc' in df and len(df) else 0.0
        c1, c2, c3 = st.columns(3)
        c1.metric("📦 Contrats (filtrés)", f"{total_contracts:,}")
        c2.metric("💰 Prime brute totale (TND)", f"{total_premium:,.0f}")
        c3.metric("⚠️ % LRC négative", f"{onerous_pct:.2f}%")

        st.success("✅ Données transformées et prêtes pour visualisation.")
        st.markdown("---")

        # ==== 🎛️ Filtres ====
        st.sidebar.header("🎛️ Filtres")
        years = df['date_effet'].dt.year.dropna()
        min_year = int(years.min()) if len(years) else 2000
        max_year = int(years.max()) if len(years) else pd.Timestamp.today().year
        year_range = st.sidebar.slider("Années d'effet (affichage)", min_year, max_year, (min_year, max_year))
        df = df[(df['date_effet'].dt.year >= year_range[0]) & (df['date_effet'].dt.year <= year_range[1])]

        if 'CODPROD' in df.columns:
            prods = st.sidebar.multiselect("Produit (CODPROD)", sorted(df['CODPROD'].dropna().unique().tolist()))
            if prods:
                df = df[df['CODPROD'].isin(prods)]

        # ==== Visualisations principales ====
        # Scatter
        st.subheader("📊 PPNA comptable vs PPNA IFRS 17 (pondération par prime)")
        df_scatter = df[(df['prime_brute'] > 0) & df['ppna_initiale'].notna() & df['ppna_ifrs17'].notna()].copy()
        fig_ppna = px.scatter(
            df_scatter,
            x='ppna_initiale',
            y='ppna_ifrs17',
            color='lrc' if 'lrc' in df_scatter else None,
            size='prime_brute',
            hover_data={
                'date_effet': True,'date_fin': True,'prime_brute': ':.2f',
                'ppna_ifrs17': ':.2f','ppna_initiale': ':.2f','lrc': ':.2f' if 'lrc' in df_scatter else False
            },
            color_continuous_scale='Viridis'
        )
        if len(df_scatter) > 0:
            fig_ppna.add_shape(
                type='line',
                x0=df_scatter['ppna_initiale'].min(),
                y0=df_scatter['ppna_initiale'].min(),
                x1=df_scatter['ppna_initiale'].max(),
                y1=df_scatter['ppna_initiale'].max(),
                line=dict(color="red", dash="dash"),
            )
        fig_ppna.update_layout(height=560)
        st.plotly_chart(fig_ppna, use_container_width=True)

        st.markdown("---")

        # Histogramme LRC
        st.subheader("📈 Distribution des LRC")
        lrc_min = float(df['lrc'].quantile(0.01)) if 'lrc' in df and len(df) else -1000.0
        lrc_max = float(df['lrc'].quantile(0.99)) if 'lrc' in df and len(df) else 1000.0
        lrc_range = st.slider("Plage LRC (pour lisibilité)", lrc_min, lrc_max, (lrc_min, lrc_max))
        hist_percent = st.checkbox("Afficher en pourcentage", value=True)

        df_hist = df[(df['lrc'] >= lrc_range[0]) & (df['lrc'] <= lrc_range[1])].copy() if 'lrc' in df else df.copy()
        fig_lrc = go.Figure()
        fig_lrc.add_trace(go.Histogram(
            x=df_hist['lrc'] if 'lrc' in df_hist else [],
            nbinsx=60,
            marker_color='#636EFA',
            name="LRC calculées",
            histnorm="percent" if hist_percent else None
        ))
        fig_lrc.add_vline(x=0, line_width=2, line_dash="dash", line_color="red")
        fig_lrc.update_layout(
            title="Distribution des LRC – ligne rouge : 0",
            xaxis_title="LRC",
            yaxis_title="Pourcentage" if hist_percent else "Nombre",
            height=460
        )
        st.plotly_chart(fig_lrc, use_container_width=True)

        st.markdown("---")

        # ==== 📅 Revenus : agrégation mensuelle (proxy) ====
        st.subheader("📆 Revenus IFRS 17 reconnus – agrégation par mois d'effet (proxy)")
        df['mois_effet'] = df['date_effet'].dt.to_period('M')
        df_rev = df.groupby('mois_effet', as_index=False)['revenue_plot'].sum()
        if len(df_rev):
            df_rev['mois_effet'] = df_rev['mois_effet'].dt.to_timestamp()

        fig_revenue = px.bar(
            df_rev,
            x='mois_effet',
            y='revenue_plot',
            title="Évolution mensuelle (revenus IFRS 17 – proxy lissé)",
            labels={'mois_effet': "Mois", 'revenue_plot': "Revenue IFRS 17 (TND)"},
            height=460
        )
        fig_revenue.update_layout(xaxis_title="Mois", yaxis_title="Montant total (TND)")
        st.plotly_chart(fig_revenue, use_container_width=True)

        # ==== 🧮 PROJECTION MENSUELLE EXACTE ====
        st.markdown("---")
        st.subheader("🧮 Projection mensuelle EXACTE (par contrat)")
        st.caption("Génère 'duree_mois' périodes depuis le mois d'effet, revenu lissé par mois.")

        # ⚙️ Limiter la projection avant export (pour volume)
        yr_min_all = int(df['date_effet'].dt.year.min())
        yr_max_all = int(df['date_effet'].dt.year.max())
        years_export = st.slider("Années d'effet à inclure dans la projection (export)", yr_min_all, yr_max_all, (yr_min_all, yr_max_all))
        df_export_proj = df[(df['date_effet'].dt.year >= years_export[0]) & (df['date_effet'].dt.year <= years_export[1])].copy()

        df_proj = monthly_projection_exact(df_export_proj)
        st.write("Aperçu projection :", df_proj.head(10))

        # ==== ⬇️ EXPORTS ====
        st.markdown("---")
        st.subheader("⬇️ Exports professionnels")

        df_rev_agg = df_rev.rename(columns={'mois_effet': 'mois'})

        # 1) Excel chunké (multi-onglets si > 1 000 000 lignes)
        try:
            xlsx_bytes = export_excel_chunked(df_main=df, df_rev_agg=df_rev_agg, df_proj=df_proj)
            st.download_button(
                label="📥 Télécharger Excel (multi-onglets) – IFRS17_PAA_Export.xlsx",
                data=xlsx_bytes,
                file_name="IFRS17_PAA_Export.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
        except Exception as ex:
            st.warning(f"Excel volumineux : {ex}")

        # 2) ZIP CSV (sans limite de lignes)
        zip_bytes = export_zip_csv({
            "IFRS17_PAA_Data": df,
            "Revenue_Mensuel_Agg": df_rev_agg,
            "Projection_Mensuelle": df_proj
        })
        st.download_button(
            label="🗜️ Télécharger ZIP (CSV) – Grand volume",
            data=zip_bytes,
            file_name="IFRS17_PAA_Export.zip",
            mime="application/zip"
        )

        # 3) PDF synthèse
        pdf_bytes = export_pdf_summary(df_main=df, df_rev_agg=df_rev_agg)
        st.download_button(
            label="📄 Télécharger PDF (Rapport_Synthese_IFRS17_PAA.pdf)",
            data=pdf_bytes,
            file_name="Rapport_Synthese_IFRS17_PAA.pdf",
            mime="application/pdf"
        )

        st.success("✅ Exports prêts.")

    except Exception as e:
        st.error(f"❌ Erreur lors du traitement du fichier : {e}")

else:
    st.warning("🕒 Veuillez importer un fichier PPNA pour commencer.")
