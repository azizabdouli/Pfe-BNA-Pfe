import pandas as pd

def prepare_ppna_data(df_raw):
    df = df_raw.copy()

    # --- Étape 1 : Conversion des dates ---
    df['date_effet'] = pd.to_datetime(df['DEBEFFQUI'], format='%Y%m%d', errors='coerce')
    df['date_fin'] = pd.to_datetime(df['FINEFFQUI'], format='%Y%m%d', errors='coerce')
    df['date_emission'] = pd.to_datetime(df['DATEEMISS'], format='%Y%m%d', errors='coerce')

    # --- Étape 2 : Durée de couverture en jours et mois ---
    df['jours_totaux'] = (df['date_fin'] - df['date_effet']).dt.days
    df['duree_mois'] = (df['jours_totaux'] / 30).round()

    # --- Étape 3 : Montant de la prime brute (MNTPRNET) ---
    df['prime_brute'] = df['MNTPRNET']

    # --- Étape 4 : PPNA actuelle locale (MNTPPNA) ---
    df['ppna_initiale'] = df['MNTPPNA']

    # --- Étape 5 : Calcul de la prime acquise (à la date actuelle) ---
    today = pd.Timestamp.today()
    df['jours_couverts'] = (today - df['date_effet']).dt.days.clip(lower=0)
    df['jours_couverts'] = df[['jours_couverts', 'jours_totaux']].min(axis=1)  # pas plus que la durée du contrat
    df['prime_acquise'] = df['prime_brute'] * (df['jours_couverts'] / df['jours_totaux'])

    # --- Étape 6 : PPNA recalculée IFRS 17 ---
    df['ppna_ifrs17'] = df['prime_brute'] - df['prime_acquise']

    # --- Étape 7 : Estimation des frais d’acquisition (10%) ---
    df['dac'] = df['prime_brute'] * 0.10

    # --- Étape 8 : LRC = PPNA - DAC
    df['lrc'] = df['ppna_ifrs17'] - df['dac']

    # --- Étape 9 : Revenu mensuel IFRS 17
    df['revenue_mensuel'] = df['lrc'] / df['duree_mois']
    df['revenue_mensuel'] = df['revenue_mensuel'].fillna(0)

    # --- Étape 10 : Filtrer les contrats valides
    df = df[df['jours_totaux'] > 0]

    return df
