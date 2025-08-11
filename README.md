# 📊 BiD Indian Biotech Startups Data extraction Pipeline

An automated pipeline for extracting, enriching, and storing structured data about **government-funded biotech startups in India** — including funding history, biotech categories, progression metrics, and news-based updates. Designed to support ROI tracking, dashboard visualizations, and policy research.

---

## ✅ Features

* 🔍 Extract data from government sources (e.g., BIRAC)
* 🌐 Enrich data using company websites and news articles
* 🧠 NLP-powered enrichment with `langchain` and `transformers`
* 🧱 Store structured data in PostgreSQL
* 🛠 Modular & extensible architecture
* ⚡ Quick setup with [`uv`](https://github.com/astral-sh/uv) and `pyproject.toml`

---

## 📁 Project Structure

```text
biotech_startup_data_pipeline/
├── pyproject.toml          # Project dependencies and metadata
├── README.md               # Project documentation
├── .venv/                  # Virtual environment (created by uv)
└── src/                    # Source code (ETL, scraping, enrichment)
```

---

## 🔧 Requirements

* Python 3.10 or later
* [`uv`](https://github.com/astral-sh/uv) — modern Python package manager
* Google Chrome
* ChromeDriver (version matching your Chrome install)

---

## 🚀 Getting Started

Follow these steps to set up and run the pipeline locally:

### 1. Install `uv`

```bash
pip install uv
# or
pipx install uv
```

### 2. Create a Virtual Environment

```bash
uv venv

```

### 3. Activate the Virtual Environment

* **macOS/Linux:**

  ```bash
  source .venv/bin/activate
  ```
* **Windows:**

  ```bash
  .venv\Scripts\activate
  ```

### 4. (Optional) Install Project Dependencies

```bash
uv pip install .
```

### 5. Sync and Lock Dependencies

```bash
uv sync
uv lock
```

---
