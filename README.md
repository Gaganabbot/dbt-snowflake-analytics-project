# Modern Analytics Engineering with dbt + Snowflake


[![dbt Version](https://img.shields.io/badge/dbt-1.7.0-orange.svg)](https://docs.getdbt.com/)
[![Snowflake](https://img.shields.io/badge/Snowflake-Ready-blue.svg)](https://www.snowflake.com/)

> **A production-ready dbt project showcasing modern analytics engineering best practices with Snowflake.**

Transform raw ecommerce and marketing data into business-ready analytics tables using a proven three-layer architecture.

## 🚀 Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/yourusername/dbt-snowflake-analytics-project.git
cd dbt-snowflake-analytics-project

# 2. Set up environment
python -m venv venv && source venv/bin/activate
pip install dbt-snowflake==1.7.0

# 3. Configure connection (see SETUP_GUIDE.md)
dbt debug

# 4. Run the project
dbt deps && dbt run && dbt test
```

## 📊 What's Included

### 🏛️ **Modern dbt Architecture**
- **Staging Layer**: 6 models for data standardization
- **Intermediate Layer**: 2 models for business logic  
- **Marts Layer**: 4 models for analytics consumption
- **Domain Organization**: Core business + Marketing analytics

### 📈 **Business Analytics**
- **Customer Intelligence**: LTV, segmentation, behavioral analysis
- **Marketing Performance**: Campaign ROI, attribution, channel optimization
- **Operational Metrics**: Order completion, payment success rates
- **Executive Dashboards**: KPI tracking and business intelligence

### 🔧 **Production Features**  
- **CI/CD Pipeline**: Automated testing with GitHub Actions
- **Data Quality**: 20+ tests with dbt-core and dbt-expectations
- **Documentation**: Auto-generated docs with business context
- **Performance**: Snowflake-optimized configurations

## 🎯 Business Value

| Analytics Domain | Key Metrics | Business Impact |
|-----------------|-------------|-----------------|
| **Customer** | LTV, Churn, Segmentation | Retention & Growth Strategy |
| **Marketing** | ROAS, Attribution, CPA | Campaign Optimization |
| **Operations** | Completion Rate, Processing Time | Efficiency Improvements |
| **Executive** | Revenue, Growth, KPIs | Strategic Decision Making |

## 🏗️ Architecture Overview

```
Raw Sources → Staging → Intermediate → Marts → BI Tools
     ↓           ↓          ↓         ↓        ↓
  Extract    Standardize  Business  Analytics Reports
                         Logic     Ready    Dashboards
```

## 📁 Project Structure

```
dbt-snowflake-analytics-project/
├── 📄 README.md                    # This file
├── 📄 SETUP_GUIDE.md               # Detailed setup instructions
├── 📄 dbt_project.yml              # dbt configuration
├── 📄 packages.yml                 # Package dependencies
│
├── 📁 .github/workflows/           # CI/CD automation
├── 📁 models/                      # dbt transformations
│   ├── 📁 staging/                # Data standardization
│   ├── 📁 intermediate/           # Business logic
│   └── 📁 marts/                  # Analytics tables
├── 📁 macros/                      # Reusable SQL functions
├── 📁 tests/                       # Data quality tests
├── 📁 seeds/                       # Reference data
├── 📁 analysis/                    # Ad-hoc queries
├── 📁 snapshots/                   # Historical captures
├── 📁 docs/                        # Documentation
└── 📁 scripts/                     # Utility scripts
```

## 🧪 Data Quality & Testing

### Test Coverage
- **Schema Tests**: Uniqueness, referential integrity, accepted values
- **Business Tests**: Custom business rules and data quality checks
- **Source Freshness**: Monitoring for stale data
- **Performance**: Query optimization and materialization strategy

## 🔄 CI/CD Pipeline

### Automated Workflows
- **Pull Request Validation**: Lint, parse, test changed models
- **Production Deployment**: Full refresh with quality gates
- **Documentation**: Auto-generated and deployed docs

## 🛠️ Development Workflow

```bash
# Start new feature
git checkout -b feature/customer-segmentation

# Develop and test locally
dbt run --select +new_model
dbt test --select new_model

# Commit and create PR
git commit -m "Add customer segmentation model"
git push origin feature/customer-segmentation
```

## 📚 Documentation

| Document | Description |
|----------|-------------|
| [SETUP_GUIDE.md](SETUP_GUIDE.md) | Complete setup instructions |
| [docs/overview.md](docs/overview.md) | Architecture deep dive |
| [docs/data_dictionary.md](docs/data_dictionary.md) | Business definitions |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Development guidelines |

