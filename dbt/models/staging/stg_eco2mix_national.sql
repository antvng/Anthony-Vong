-- Nettoyage et typage des données nationales brutes
-- ELECTRICITY_RAW.ECO2MIX.RAW_NATIONAL_HISTORIQUE

with source as (

    select *
    from ELECTRICITY_RAW.ECO2MIX.RAW_NATIONAL_HISTORIQUE

),

cleaned as (

    select
        -- Horodatage
        try_to_timestamp("date_heure")              as date_heure,

        -- Consommation et prévisions (MW)
        "consommation"::float                       as consommation_mw,
        "prevision_j1"::float                       as prevision_j1_mw,
        "prevision_j"::float                        as prevision_j_mw,

        -- Mix de production par filière (MW)
        "nucleaire"::float                          as nucleaire_mw,
        "hydraulique"::float                        as hydraulique_mw,
        "eolien"::float                             as eolien_mw,
        "solaire"::float                            as solaire_mw,
        "gaz"::float                                as gaz_mw,
        "fioul"::float                              as fioul_mw,
        "charbon"::float                            as charbon_mw,
        "bioenergies"::float                        as bioenergies_mw,
        "pompage"::float                            as pompage_mw,
        "ech_physiques"::float                      as ech_physiques_mw,

        -- Échanges commerciaux
        "ech_comm_angleterre"::float                as ech_angleterre_mw,
        "ech_comm_espagne"::float                   as ech_espagne_mw,
        "ech_comm_italie"::float                    as ech_italie_mw,
        "ech_comm_suisse"::float                    as ech_suisse_mw,
        "ech_comm_allemagne_belgique"::float        as ech_allemagne_belgique_mw,

        -- KPI central
        "taux_co2"::float                           as taux_co2,

        -- Métadonnées
        "_loaded_at"                                as _loaded_at

    from source

),

final as (

    select *
    from cleaned
    where date_heure is not null
    and consommation_mw is not null
    and consommation_mw > 0

)

select * from final