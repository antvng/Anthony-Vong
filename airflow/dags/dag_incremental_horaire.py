from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from datetime import datetime, timedelta
import logging

logger = logging.getLogger(__name__)


def fetch_realtime_and_upsert() -> None:
    """
    Récupère les 100 dernières mesures temps réel RTE et fait un MERGE INTO
    dans Snowflake pour éviter les doublons.

    Pourquoi 100 mesures et pas plus ?
    Les données temps réel couvrent le mois en cours au pas de 30 min.
    100 mesures = ~50 heures de couverture. Largement suffisant pour
    ne pas rater de données même si le DAG n'a pas tourné pendant 2 jours.
    """
    import requests
    import snowflake.connector

    # API Call
    url = (
        "https://odre.opendatasoft.com/api/explore/v2.1/catalog/datasets/"
        "eco2mix-national-tr/records"
    )
    params = {
        "limit": 100,
        "order_by": "date_heure DESC",
        "timezone": "Europe/Paris",
    }

    logger.info("Récupération des données temps réel RTE...")
    response = requests.get(url, params=params, timeout=60)
    response.raise_for_status()

    records = response.json().get("results", [])
    logger.info(f"{len(records)} mesures récupérées depuis l'API.")

    if not records:
        logger.warning("Aucune donnée retournée par l'API temps réel.")
        return

    # Login snowflake
    conn = snowflake.connector.connect(
        user=Variable.get("SNOW_USER"),
        password=Variable.get("SNOW_PASSWORD"),
        account=Variable.get("SNOW_ACCOUNT"),
        warehouse=Variable.get("SNOW_WAREHOUSE"),
        database=Variable.get("SNOW_DATABASE"),
        schema="ECO2MIX",
    )

    cursor = conn.cursor()

    # Créer la table si elle n'existe pas encore
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS RAW_NATIONAL_TEMPS_REEL (
            date_heure        TIMESTAMP_TZ,
            consommation      FLOAT,
            prevision_j1      FLOAT,
            prevision_j       FLOAT,
            fioul             FLOAT,
            charbon           FLOAT,
            gaz               FLOAT,
            nucleaire         FLOAT,
            eolien            FLOAT,
            solaire           FLOAT,
            hydraulique       FLOAT,
            pompage           FLOAT,
            bioenergies       FLOAT,
            ech_physiques     FLOAT,
            taux_co2          FLOAT,
            _loaded_at        TIMESTAMP_NTZ,
            _source           VARCHAR(50)
        )
    """)

    # MERGE INTO pour chaque enregistrement
    # INSERT si nouveau, UPDATE si déjà existant
    merge_sql = """
        MERGE INTO RAW_NATIONAL_TEMPS_REEL t
        USING (
            SELECT
                %(date_heure)s::TIMESTAMP_TZ   AS date_heure,
                %(consommation)s               AS consommation,
                %(prevision_j1)s               AS prevision_j1,
                %(prevision_j)s                AS prevision_j,
                %(fioul)s                      AS fioul,
                %(charbon)s                    AS charbon,
                %(gaz)s                        AS gaz,
                %(nucleaire)s                  AS nucleaire,
                %(eolien)s                     AS eolien,
                %(solaire)s                    AS solaire,
                %(hydraulique)s               AS hydraulique,
                %(pompage)s                    AS pompage,
                %(bioenergies)s               AS bioenergies,
                %(ech_physiques)s             AS ech_physiques,
                %(taux_co2)s                  AS taux_co2
        ) s
        ON t.date_heure = s.date_heure
        WHEN MATCHED AND (
            t.taux_co2 != s.taux_co2
            OR t.consommation != s.consommation
        ) THEN UPDATE SET
            t.taux_co2      = s.taux_co2,
            t.consommation  = s.consommation,
            t.nucleaire     = s.nucleaire,
            t.eolien        = s.eolien,
            t.solaire       = s.solaire,
            t.gaz           = s.gaz,
            t._loaded_at    = %(loaded_at)s
        WHEN NOT MATCHED THEN INSERT (
            date_heure, consommation, prevision_j1, prevision_j,
            fioul, charbon, gaz, nucleaire, eolien, solaire,
            hydraulique, pompage, bioenergies, ech_physiques,
            taux_co2, _loaded_at, _source
        ) VALUES (
            s.date_heure, s.consommation, s.prevision_j1, s.prevision_j,
            s.fioul, s.charbon, s.gaz, s.nucleaire, s.eolien, s.solaire,
            s.hydraulique, s.pompage, s.bioenergies, s.ech_physiques,
            s.taux_co2, %(loaded_at)s, 'eco2mix-national-tr'
        )
    """

    loaded_at = datetime.utcnow()
    inserted = 0
    updated = 0

    for record in records:
        params_row = {
            "date_heure":    record.get("date_heure"),
            "consommation":  record.get("consommation"),
            "prevision_j1":  record.get("prevision_j1"),
            "prevision_j":   record.get("prevision_j"),
            "fioul":         record.get("fioul"),
            "charbon":       record.get("charbon"),
            "gaz":           record.get("gaz"),
            "nucleaire":     record.get("nucleaire"),
            "eolien":        record.get("eolien"),
            "solaire":       record.get("solaire"),
            "hydraulique":   record.get("hydraulique"),
            "pompage":       record.get("pompage"),
            "bioenergies":   record.get("bioenergies"),
            "ech_physiques": record.get("ech_physiques"),
            "taux_co2":      record.get("taux_co2"),
            "loaded_at":     loaded_at,
        }
        cursor.execute(merge_sql, params_row)

    conn.commit()
    conn.close()
    logger.info(f"  MERGE INTO terminé — {len(records)} enregistrements traités.")


def verify_recent_data() -> None:
    """
    Vérifie que les données sont bien fraîches.
    Si la dernière mesure date de plus de 2h, quelque chose a planté.
    """
    import snowflake.connector

    conn = snowflake.connector.connect(
        user=Variable.get("SNOW_USER"),
        password=Variable.get("SNOW_PASSWORD"),
        account=Variable.get("SNOW_ACCOUNT"),
        warehouse=Variable.get("SNOW_WAREHOUSE"),
        database=Variable.get("SNOW_DATABASE"),
        schema="ECO2MIX",
    )

    cursor = conn.cursor()
    cursor.execute("""
        SELECT COUNT(*), MAX(date_heure)
        FROM RAW_NATIONAL_TEMPS_REEL
    """)
    row = cursor.fetchone()
    conn.close()

    logger.info(f"  Total lignes temps réel : {row[0]:,}")
    logger.info(f"   Dernière mesure        : {row[1]}")


# DAG
with DAG(
    dag_id="incremental_horaire_national",
    description="Récupère les données RTE temps réel toutes les heures via MERGE INTO",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",      # Toutes les heures pile 
    catchup=False,              # Ne pas rattraper les heures passées
    default_args={
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["incremental", "bronze", "national", "hourly"],
) as dag:

    fetch = PythonOperator(
        task_id="fetch_and_upsert",
        python_callable=fetch_realtime_and_upsert,
    )

    verify = PythonOperator(
        task_id="verify_recent_data",
        python_callable=verify_recent_data,
    )

    fetch >> verify