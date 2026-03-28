-- models/marts/mart_national_horaire.sql
-- Une ligne par mesure 30min avec toutes les métriques CO2 + mix
-- Modèle INCREMENTAL

{{
    config(
        materialized='incremental',
        unique_key='date_heure',
        on_schema_change='sync_all_columns'
    )
}}

with source as (

    select *
    from {{ ref('int_national_enrichi') }}

    -- INCREMENTAL si table existe dans snowflake
    -- dbt filtre que sur les nouv lignes
    {% if is_incremental() %}
        where date_heure > (select max(date_heure) from {{ this }})
    {% endif %}

),

final as (

    select
        -- Primary Key
        date_heure,

        -- Dimensions temporelles
        heure,
        saison,
        jour_semaine,
        mois,
        annee,
        flag_weekend,
        flag_covid,
        flag_crise_nuc_2022,

        -- Consommation et prévisions
        consommation_mw,
        prevision_j1_mw,
        prevision_j_mw,

        -- Écart prévision vs réel
        round(consommation_mw - prevision_j_mw, 2)     as ecart_prevision_mw,

        -- Prod par filière
        nucleaire_mw,
        hydraulique_mw,
        eolien_mw,
        solaire_mw,
        gaz_mw,
        fioul_mw,
        charbon_mw,
        bioenergies_mw,
        pompage_mw,
        ech_physiques_mw,

        -- KPIs CO2
        taux_co2,
        indice_vert,
        part_bas_carbone,
        part_renouvelables,
        part_fossiles,

        -- Métadata
        _loaded_at

    from source

)

select * from final

-- Premier run create tables et check 490k lignes
-- Run suivants : traite les nouvelles lignes