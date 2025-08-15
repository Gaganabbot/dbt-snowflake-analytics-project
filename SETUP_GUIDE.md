# dbt + Snowflake Setup Guide

## Prerequisites

### Required Software
- Python 3.9 or higher
- Git installed and configured
- Access to a Snowflake account
- Code editor (VS Code recommended)

### Snowflake Requirements
- Snowflake account with appropriate privileges
- User account with CREATE DATABASE, CREATE SCHEMA permissions
- Warehouse with compute resources
- Network access to Snowflake

## Step-by-Step Setup

### 1. Clone the Project
```bash
git clone https://github.com/yourusername/dbt-snowflake-analytics-project.git
cd dbt-snowflake-analytics-project

python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate     # Windows
```

### 2. Install dbt
```bash
pip install dbt-snowflake==1.7.0
dbt --version
```

### 3. Configure Snowflake Connection

#### Environment Variables (Recommended)
```bash
export SNOWFLAKE_ACCOUNT="your_account.region.cloud"
export SNOWFLAKE_USER="your_username"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="your_role"
export SNOWFLAKE_DATABASE="your_database"
export SNOWFLAKE_WAREHOUSE="your_warehouse"
export SNOWFLAKE_SCHEMA="your_schema"
```

#### profiles.yml (Development)
```bash
mkdir -p ~/.dbt
cp profiles.yml ~/.dbt/profiles.yml
# Edit ~/.dbt/profiles.yml with your credentials
```

### 4. Prepare Snowflake Environment
```sql
CREATE DATABASE IF NOT EXISTS analytics_dev;
CREATE DATABASE IF NOT EXISTS analytics_prod;
CREATE DATABASE IF NOT EXISTS raw_data;

CREATE SCHEMA IF NOT EXISTS analytics_dev.staging;
CREATE SCHEMA IF NOT EXISTS analytics_dev.intermediate;
CREATE SCHEMA IF NOT EXISTS analytics_dev.marts;
```

### 5. Install and Test
```bash
dbt deps
dbt debug
dbt parse
```

### 6. Run the Project
```bash
dbt seed
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## GitHub Repository Setup

### 1. Create Repository
```bash
git init
git remote add origin https://github.com/yourusername/dbt-snowflake-analytics-project.git
git add .
git commit -m "Initial commit: dbt + Snowflake analytics project"
git push -u origin main
```

### 2. Set Up GitHub Secrets
Add these secrets in GitHub repository settings:

```
SNOWFLAKE_ACCOUNT
SNOWFLAKE_USER
SNOWFLAKE_PASSWORD
SNOWFLAKE_ROLE
SNOWFLAKE_DATABASE
SNOWFLAKE_WAREHOUSE
SNOWFLAKE_SCHEMA

# Production secrets
SNOWFLAKE_PROD_ACCOUNT
SNOWFLAKE_PROD_USER
SNOWFLAKE_PROD_PASSWORD
SNOWFLAKE_PROD_ROLE
SNOWFLAKE_PROD_DATABASE
SNOWFLAKE_PROD_WAREHOUSE
SNOWFLAKE_PROD_SCHEMA
```

### 3. Test CI/CD Pipeline
```bash
git checkout -b test-cicd
echo "-- Test comment" >> models/staging/ecommerce/stg_ecommerce__customers.sql
git add .
git commit -m "Test CI/CD pipeline"
git push origin test-cicd
```

## Development Workflow

### Daily Development
```bash
git checkout main
git pull origin main
git checkout -b feature/my-new-feature

# Make changes and test
dbt run --select +my_new_model
dbt test --select my_new_model

git add .
git commit -m "Add new model"
git push origin feature/my-new-feature
```

## Troubleshooting

### Connection Issues
```bash
ping your_account.snowflakecomputing.com
dbt debug --config-dir ~/.dbt
```

### Model Errors
```bash
dbt parse --warn-error
dbt compile --select my_model
```

### Test Failures
```bash
dbt test --store-failures
dbt test --select test_name --verbose
```

## Next Steps

1. Update source definitions to match your data
2. Modify models for your business requirements
3. Add new data sources following established patterns
4. Customize tests for your data quality needs
5. Set up production deployment with service accounts

---

🎉 **Congratulations!** Your dbt + Snowflake project is ready for production use.
