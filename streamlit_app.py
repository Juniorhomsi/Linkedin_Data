import streamlit as st
from snowflake.snowpark.context import get_active_session

st.title("📊 LinkedIn Job Market Analysis")
st.write("Analyse des offres d'emploi LinkedIn")

# Connexion
session = get_active_session()

st.header("1. Top 10 des titres de postes les plus publiés par industrie")

sql = """
SELECT
  industry_id,
  job_title,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES
WHERE job_title IS NOT NULL
GROUP BY industry_id, job_title
ORDER BY nb_offres DESC
LIMIT 10
"""

df = session.sql(sql).to_pandas()

st.bar_chart(df, x="JOB_TITLE", y="NB_OFFRES")
st.dataframe(df, use_container_width=True)

st.header("2. Top 10 des postes les mieux rémunérés par industrie")

sql = """
SELECT
  industry_id,
  job_title,
  AVG(med_salary) AS salaire_moyen
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES
WHERE med_salary IS NOT NULL
GROUP BY industry_id, job_title
ORDER BY salaire_moyen DESC
LIMIT 10
"""

df = session.sql(sql).to_pandas()

st.bar_chart(df, x="JOB_TITLE", y="SALAIRE_MOYEN")
st.dataframe(df, use_container_width=True)

st.header("3. Répartition des offres d’emploi par taille d’entreprise")

sql = """
SELECT
  company_size_label,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS
GROUP BY company_size_label
ORDER BY nb_offres DESC
"""

df = session.sql(sql).to_pandas()

st.bar_chart(df, x="COMPANY_SIZE_LABEL", y="NB_OFFRES")
st.dataframe(df, use_container_width=True)

st.header("4. Répartition des offres d’emploi par secteur d’activité")

sql = """
SELECT
  ci.industry AS secteur_activite,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.FACT_JOB_POSTINGS jp
LEFT JOIN LINKEDIN.SILVER.COMPANY_INDUSTRIES ci
  ON jp.company_id = ci.company_id
WHERE ci.industry IS NOT NULL
GROUP BY ci.industry
ORDER BY nb_offres DESC
"""

df = session.sql(sql).to_pandas()

st.bar_chart(df, x="SECTEUR_ACTIVITE", y="NB_OFFRES")
st.dataframe(df, use_container_width=True)


st.header("5. Répartition des offres d’emploi par type d’emploi")

sql = """
SELECT
  formatted_work_type,
  COUNT(*) AS nb_offres
FROM LINKEDIN.GOLD.DIM_JOBS
GROUP BY formatted_work_type
ORDER BY nb_offres DESC
"""

df = session.sql(sql).to_pandas()

st.bar_chart(df, x="FORMATTED_WORK_TYPE", y="NB_OFFRES")
st.dataframe(df, use_container_width=True)
