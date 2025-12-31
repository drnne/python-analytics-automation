# Python Analytics & Automation

This repository demonstrates a practical, production-style analytics workflow using **Python** to:
- extract data from **SQL Server** and/or a **REST API**
- clean and transform data into analysis-ready tables
- run validation checks (data quality)
- generate **SPC metrics** (mean, standard deviation, 2 SD warning and 3 SD control limits)
- produce outputs suitable for dashboards, reporting, or downstream pipelines

The code is deliberately structured for contract delivery:
- readable modules
- logging for traceability
- clear configuration and handover

> Notes: All examples can run on synthetic data. SQL connectivity is optional.

---

## Quick start

### 1) Create a virtual environment
```bash
python -m venv .venv
# Windows:
.venv\Scripts\activate
# Mac/Linux:
source .venv/bin/activate
