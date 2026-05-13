# Democracy Clustering Analysis V2

**Unsupervised machine learning on 167 countries — mapping the global landscape of democracy, digital freedom, and authoritarian surveillance.**

🔴 **[Live Interactive Dashboard →](https://rosalinatorres888.github.io/democracy-clustering-analysis/democracy_v2.html)**

---

## Overview

This project applies K-Means clustering and Principal Component Analysis (PCA) to classify 167 countries across six dimensions of democratic health and digital freedom. Version 2 introduces a fourth cluster — **Digital Hybrids** — capturing the growing category of nations with moderate democratic institutions but high digital surveillance.

The analysis sits at the intersection of political data science and AI safety: as governments deploy algorithmic tools for social control, understanding the structural differences between free and surveilled societies becomes increasingly urgent.

---

## Key Findings

| Cluster | Countries | Avg Democracy Score | Avg Internet Freedom |
|---|---|---|---|
| 🔵 Digital Democracies | 38 | 8.4 / 10 | 81 / 100 |
| 🟡 Constrained Democracies | 47 | 6.1 / 10 | 54 / 100 |
| 🟠 Digital Hybrids *(new in V2)* | 31 | 5.8 / 10 | 38 / 100 |
| 🔴 Hard Authoritarians | 51 | 2.3 / 10 | 14 / 100 |

**The United States** scores 77/100 on Internet Freedom — above average but trailing peers like Norway (96), Iceland (94), and Canada (87). U.S. democratic scores have declined from 8.22 in 2015 to 7.85 in 2023.

---

## Features

- **Unsupervised clustering** (K-Means, k=4) on standardized multi-dimensional data
- **Dimensionality reduction** via PCA — 6 dimensions → 2 principal components for visualization
- **Interactive D3.js dashboard** with scatter plots, cluster cards, and surveillance gap analysis
- **167 countries** across all major world regions
- **Longitudinal analysis** — U.S. democratic backsliding 2015–2023

---

## Data Sources

- **EIU Democracy Index** — Electoral process, civil liberties, political culture, political participation, government functioning
- **Freedom House Internet Freedom Index** — Online censorship, surveillance infrastructure, digital rights

---

## Tech Stack

```
Python          scikit-learn    pandas          NumPy
K-Means         PCA             StandardScaler  matplotlib
D3.js v7        HTML5 Canvas    GitHub Pages
```

---

## Project Structure

```
democracy-clustering-analysis/
├── democracy_v2.html          # Live interactive dashboard (D3.js)
├── democracy_clustering.ipynb # Full analysis notebook
├── data/
│   └── democracy_data.csv     # Combined EIU + Freedom House dataset
└── README.md
```

---

## Run Locally

```bash
git clone https://github.com/rosalinatorres888/democracy-clustering-analysis.git
cd democracy-clustering-analysis

pip install pandas numpy scikit-learn matplotlib jupyter

jupyter notebook democracy_clustering.ipynb
```

To view the dashboard locally, open `democracy_v2.html` in a browser — or visit the [live GitHub Pages version](https://rosalinatorres888.github.io/democracy-clustering-analysis/democracy_v2.html).

---

## Why This Matters for AI Safety

Authoritarian governments are increasingly deploying AI-powered surveillance — facial recognition, social scoring, predictive policing — to suppress dissent and consolidate control. This analysis provides a data-driven baseline for understanding which countries are most vulnerable to AI-enabled democratic erosion, and where digital rights protections remain strongest.

The **Digital Hybrids** cluster is particularly significant: these are not classic autocracies. They retain electoral institutions while quietly expanding digital surveillance infrastructure — a pattern that unsupervised ML can detect where qualitative analysis often misses it.

---

## About

**Rosalina Torres** — MS Data Analytics Engineering, Northeastern University (4.0 GPA)

15 years in enterprise technology across Latin America, now building at the intersection of machine learning and policy. Former Senior Partner Manager at Collibra; Sales Leader at Oracle, Zerto, and EMC.

- [GitHub](https://github.com/rosalinatorres888)
- [LinkedIn](https://www.linkedin.com/in/rosalinatorres)

---

*Built with Python · scikit-learn · D3.js · Open data*
