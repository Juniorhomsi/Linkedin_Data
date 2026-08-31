# Linkedin_Data
# 📊 Analyse des Offres d'Emploi LinkedIn avec Snowflake

> Un projet d'architecture Big Data complet utilisant la méthodologie Medallion (Bronze/Silver/Gold) pour l'analyse et la visualisation d'offres d'emploi LinkedIn.

## 🎯 Vue d'ensemble

Ce projet démontre une implémentation complète d'un pipeline de données modernes utilisant:
- **Snowflake** comme entrepôt de données cloud
- **Architecture Medallion** pour l'organisation et la qualité des données
- **Streamlit** pour les visualisations interactives
- **SQL et Python** pour l'ingénierie des données

Les données proviennent du bucket S3 public contenant plusieurs milliers d'offres d'emploi LinkedIn avec des informations détaillées sur les salaires, localisation, compétences requises et avantages sociaux.

---

## 📁 Structure du Projet

```
Linkedin_Data/
├── LinkedinProject.sql       # Script SQL complet (schema, ETL, transformations)
├── streamlit_app.py          # Application Streamlit pour la visualisation
├── Rapport_final             # Documentation détaillée du projet
├── README.md                 # Ce fichier
└── images/                   # Captures d'écran de l'analyse
    ├── image-1.png
    ├── image-2.png
    ├── image-3.png
    ├── image-4.png
    └── image-5.png
```

---

## 🔧 Technologies Utilisées

| Composant | Technologie |
|-----------|------------|
| **Data Warehouse** | Snowflake (Cloud) |
| **Storage** | Amazon S3 |
| **Backend** | SQL + Python |
| **Visualisation** | Streamlit |
| **Architecture** | Medallion (Bronze/Silver/Gold) |

---

## 📊 Datasets

Les données proviennent du bucket S3 public : `s3://snowflake-lab-bucket/`

### Fichiers CSV
- **job_postings.csv** — Offres d'emploi (titre, salaire, type de contrat, localisation, etc.)
- **benefits.csv** — Avantages sociaux associés à chaque offre
- **employee_counts.csv** — Nombre d'employés et followers par entreprise
- **job_skills.csv** — Compétences requises par offre d'emploi

### Fichiers JSON
- **companies.json** — Informations détaillées sur les entreprises
- **company_industries.json** — Secteurs d'activité par entreprise
- **company_specialities.json** — Spécialités par entreprise
- **job_industries.json** — Secteurs d'activité par offre d'emploi

---

## 🏗️ Architecture Medallion

### 🥉 Couche BRONZE — Données Brutes
Chargement intégral des fichiers S3 sans transformation :
- Toutes les colonnes en type `STRING`
- 8 tables Bronze correspondant aux 8 fichiers source
- Garantit la traçabilité des données originales

### 🥈 Couche SILVER — Données Nettoyées
Transformation et nettoyage des données :
- Suppression des enregistrements invalides (job_id, titre ou entreprise manquants)
- Conversion des types de données (dates, nombres, booléens)
- Extraction des champs JSON en colonnes distinctes
- Validation des salaires (exclusion des valeurs nulles ou négatives)
- Enrichissement avec des colonnes calculées

### 🥇 Couche GOLD — Données Modélisées
Schéma analytique optimisé avec modèle dimensionnel :
- **Tables de faits** : `FACT_JOB_INDUSTRIES`, `FACT_JOBS`
- **Tables de dimensions** : `DIM_COMPANIES`, `DIM_LOCATIONS`, `DIM_SKILLS`
- Prêtes pour la visualisation et l'analyse

---

## 📈 Analyses Disponibles

L'application Streamlit fournit les analyses suivantes :

1. **Top 10 des titres de postes les plus publiés** par industrie
2. **Top 10 des postes les mieux rémunérés** par industrie
3. **Répartition des offres d'emploi** par taille d'entreprise
4. **Salaires moyens** par niveau d'expérience requis
5. **Tendances** de rémunération par secteur
6. **Analyse des compétences** les plus demandées

---

## 🚀 Démarrage Rapide

### Prérequis
- ✅ Compte Snowflake actif
- ✅ Accès au bucket S3 public `snowflake-lab-bucket`
- ✅ Python 3.8+
- ✅ Streamlit et pilotes Snowflake installés

### Installation

```bash
# Installer les dépendances Python
pip install streamlit snowflake-snowpark-python

# Cloner ou télécharger le projet
git clone <repository-url>
cd Linkedin_Data
```

### Configuration

1. **Exécuter le script SQL** (`LinkedinProject.sql`) dans Snowflake :
   - Crée la base de données `LINKEDIN`
   - Configure le warehouse
   - Charge les données depuis S3
   - Crée les couches Silver et Gold

2. **Lancer l'application Streamlit** :
   ```bash
   streamlit run streamlit_app.py
   ```

3. **Configurer la connexion Snowflake** :
   - Authentifiez-vous avec vos identifiants Snowflake
   - L'application se connectera automatiquement

---

## 📋 Contenu des Fichiers

### `LinkedinProject.sql`
Script SQL complet (~500+ lignes) incluant :
- Création de la base de données et des schémas
- Configuration des stages et formats de fichiers
- Création de warehouse optimisé
- Chargement des données Bronze
- Transformations Silver (nettoyage et enrichissement)
- Modèle dimensionnel Gold (faits et dimensions)

### `streamlit_app.py`
Application web interactive avec :
- Interface utilisateur intuitive
- Graphiques dynamiques (bar charts, dataframes)
- Requêtes SQL optimisées
- Rafraîchissement en temps réel des données

### `Rapport_final`
Documentation complète incluant :
- Objectifs du projet
- Description détaillée des données
- Architecture Medallion expliquée
- Analyses réalisées
- Problèmes rencontrés et solutions
- Résultats et insights

---

## 💡 Points Clés

- ✅ **Pipeline complet** : Du chargement brut à la visualisation
- ✅ **Architecture scalable** : Medallion pattern pour la qualité des données
- ✅ **Code production-ready** : SQL optimisé et best practices
- ✅ **Pas d'interface graphique** : 100% scripts (SQL + Python)
- ✅ **Données réelles** : Offres LinkedIn actuelles

---

## 📊 Exemples de Résultats

L'analyse révèle :
- Les industries avec le **plus d'offres** d'emploi
- Les postes **les mieux payés** dans chaque secteur
- La **corrélation** entre expérience requise et salaire
- Les **compétences** les plus demandées
- Les **tendances** géographiques de l'emploi

---

## 🔍 Architecture Technique Détaillée

```
┌─────────────────────────────────────────┐
│         Source S3 (Données brutes)      │
│  job_postings.csv, benefits.csv, ...    │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│    Couche BRONZE (Chargement brut)      │
│    8 tables : toutes les colonnes       │
│    en type STRING                        │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│   Couche SILVER (Transformation)        │
│   - Nettoyage des données               │
│   - Conversion de types                 │
│   - Extraction JSON                     │
│   - Validation des règles métier        │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│    Couche GOLD (Modèle analytique)      │
│    - Tables de faits                    │
│    - Tables de dimensions               │
│    - Prête pour l'analyse               │
└──────────────────┬──────────────────────┘
                   │
                   ▼
┌─────────────────────────────────────────┐
│  Streamlit (Visualisation interactive)  │
│  Dashboards, graphiques, tableaux       │
└─────────────────────────────────────────┘
```

---

## 📝 Exemple de Requête

```python
# Obtenir les top 10 meilleurs salaires par industrie
SELECT
  industry_id,
  job_title,
  AVG(med_salary) AS salaire_moyen
FROM LINKEDIN.GOLD.FACT_JOB_INDUSTRIES
WHERE med_salary IS NOT NULL
GROUP BY industry_id, job_title
ORDER BY salaire_moyen DESC
LIMIT 10;
```

---

## 🎓 Apprentissages Clés

Ce projet démontre :
- La mise en pratique de l'**architecture Medallion**
- L'utilisation de **Snowflake** pour le cloud analytics
- L'intégration d'**S3** pour le stockage de données
- La transformation de données brutes en insights actionnables
- Les best practices en **ingénierie des données**

---

## 📚 Pour Aller Plus Loin

- Consulter `Rapport_final` pour la documentation complète
- Voir les captures d'écran (image-1.png à image-5.png) pour les visualisations
- Analyser le SQL dans `LinkedinProject.sql` pour comprendre les transformations
- Explorer les requêtes Streamlit dans `streamlit_app.py`

---

## 👥 Auteurs

Projet réalisé dans le cadre d'une **évaluation d'architecture Big Data**

**Groupe :** Manuella & Pierre  
**Référence :** MBAESG_EVALUATION_ARCHITECTURE_BIGDATA

---

## 📄 Licence

Ce projet est fourni à des fins éducatives.

---

## 🤝 Support

Pour des questions ou des améliorations, consultez la documentation détaillée dans `Rapport_final`.

---

**Dernier mise à jour :** 2026
