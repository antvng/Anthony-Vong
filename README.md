# Electricity CO2 Pipeline

![dbt CI](https://github.com/antvng/Electricity-co2-pipeline/actions/workflows/dbt_ci.yml/badge.svg)

Pipeline de données sur l'empreinte carbone de l'électricité française.  
"En été, charger sa voiture électrique à 12h plutôt qu'en soirée, c'est 48% de CO2 en moins."

---

## C'est quoi ce projet ?

J'ai construit un pipeline end-to-end qui répond à une question simple : quand et où l'électricité française est-elle vraiment verte ?

Les données viennent directement de l'API officielle RTE éco2mix — des mesures réelles au pas de 30 minutes depuis 2012, mises à jour toutes les heures via un DAG Airflow.

---

## Stack

 Outil & Rôles

**Apache Airflow** | Orchestration des DAGs |
**Snowflake** | Data Warehouse |
**dbt Core** | Transformation & tests |
**Power BI Service** | Dashboard live |
**Docker** | Environnement local |

---

## Architecture

RTE éco2mix API → Airflow (4 DAGs) → Snowflake Bronze → dbt → Power BI

---

## Dashboard Power BI

Page 1 — L'électricité est-elle verte en ce moment ?
![Dashboard Indice Vert](docs/screenshots/Dashboard_1_Indice_Vert.png)

Page 2 — Quand l'électricité est-elle la plus verte ?
![Dashboard Patterns Temporels](docs/screenshots/Dashboard_2_Patterns.png)

Page 3 — Quelle région produit l'électricité la plus verte ?
![Dashboard Mix Régional](docs/screenshots/Dashboard_3_Regional.png)

Page 4 — À quelle heure charger sa voiture électrique ?
![Dashboard Guide Pratique](docs/screenshots/Dashboard_4_Guide_Zoe.png)

---

## Pipeline & infrastructure

Airflow — 4 DAGs
![Airflow DAGs](docs/screenshots/Airflow_DAGs.png)

Snowflake — Architecture médaillon
![Snowflake Architecture](docs/screenshots/Snowflake_Architecture.png)

---

## Modèles dbt

- `staging/` — nettoyage et typage des données brutes
- `intermediate/` — enrichissement temporel et calcul des KPIs CO2
- `marts/` — tables analytiques prêtes pour Power BI

245 638 lignes · 2012 → live · 37 tests · CI/CD GitHub Actions

---

## Lancer le projet

Cloner
git clone https://github.com/antvng/Electricity-co2-pipeline.git
cd Electricity-co2-pipeline

Configurer les credentials
cp .env.example .env

Lancer Airflow
cd docker && docker compose up -d

Installer dbt et lancer les modèles
python -m venv .venv && source .venv/bin/activate
pip install dbt-snowflake==1.8.0
cd dbt && dbt run && dbt test