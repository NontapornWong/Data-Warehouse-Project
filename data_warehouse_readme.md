# Data Warehouse Portfolio Project

A complete data warehouse implementation using PostgreSQL, Docker, and Python - demonstrating modern data engineering practices.

## 🏗️ Architecture Overview

```
Python Data Generation → PostgreSQL Data Warehouse → Analytics & BI
         ↓                        ↓                      ↓
    Faker Library          Docker Container        Business Intelligence
    100k+ Records         Star Schema Design       Performance Queries
```

## 📊 Data Model (Star Schema)

### Fact Table
- **sales_transactions** (85,000+ records)
  - Transaction details with foreign keys to dimensions
  - Measures: quantity, unit_price, total_amount, discount_amount

### Dimension Tables
- **customers** (5,000 records) - Customer demographics and segments
- **products** (500 records) - Product catalog with categories and pricing
- **date_dimension** (731 records) - Time dimension for 2023-2024

## 🛠️ Technology Stack

- **Database**: PostgreSQL 15 (Alpine)
- **Containerization**: Docker & Docker Compose
- **Data Generation**: Python (Faker, Pandas, NumPy)
- **Database Connectivity**: psycopg2
- **Analytics**: SQL & Python pandas

## 🚀 Quick Start

### Prerequisites
- Docker Desktop
- Python 3.8+
- 4GB+ available RAM

### Setup & Run
```bash
# Clone and navigate to project
git clone <your-repo>
cd data-warehouse-project

# Start database services
docker-compose up -d

# Create Python virtual environment
python -m venv venv
source venv/bin/activate  # or venv\Scripts\activate on Windows

# Install dependencies
pip install -r python/requirements.txt

# Generate sample data
python python/generate_data.py

# Load data into warehouse
python python/load_data.py

# Run analytics dashboard
python python/analytics.py
```

## 📈 Sample Analytics Queries

### Monthly Revenue Trends
```sql
SELECT 
    d.year, d.month,
    SUM(st.total_amount) as monthly_revenue,
    COUNT(*) as total_transactions
FROM sales_transactions st
JOIN date_dimension d ON st.date_id = d.date_id
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```

### Customer Segment Performance
```sql
SELECT 
    c.customer_segment,
    COUNT(DISTINCT c.customer_id) as customers,
    SUM(st.total_amount) as total_revenue,
    AVG(st.total_amount) as avg_order_value
FROM sales_transactions st
JOIN customers c ON st.customer_id = c.customer_id
GROUP BY c.customer_segment;
```

### Top Products by Category
```sql
SELECT 
    p.category,
    p.product_name,
    SUM(st.total_amount) as revenue,
    SUM(st.quantity) as units_sold
FROM sales_transactions st
JOIN products p ON st.product_id = p.product_id
GROUP BY p.category, p.product_name
ORDER BY p.category, revenue DESC;
```

## 🎯 Key Features Demonstrated

### Data Engineering
- ✅ **ETL Pipeline**: Extract (CSV), Transform (Python), Load (PostgreSQL)
- ✅ **Data Modeling**: Star schema with fact and dimension tables
- ✅ **Data Generation**: Realistic synthetic data using Faker
- ✅ **Batch Processing**: Efficient bulk data loading

### Database Design
- ✅ **Normalized Design**: Proper foreign key relationships
- ✅ **Performance Optimization**: Strategic indexes on frequently queried columns
- ✅ **Data Types**: Appropriate column types and constraints
- ✅ **Referential Integrity**: Enforced relationships between tables

### Infrastructure
- ✅ **Containerization**: Docker Compose for reproducible environment
- ✅ **Service Management**: Multi-container orchestration
- ✅ **Volume Persistence**: Data survives container restarts
- ✅ **Network Security**: Isolated Docker network

### Analytics & BI
- ✅ **Aggregation Queries**: SUM, COUNT, AVG across dimensions
- ✅ **Time Series Analysis**: Monthly/quarterly reporting
- ✅ **Customer Segmentation**: Behavioral analysis
- ✅ **Product Performance**: Category and brand analysis

## 📁 Project Structure

```
data-warehouse-project/
├── docker-compose.yml          # Container orchestration
├── README.md                   # This file
├── sql/
│   ├── schema.sql             # Table definitions
│   └── indexes.sql            # Performance indexes
├── python/
│   ├── .env                   # Database configuration
│   ├── requirements.txt       # Python dependencies
│   ├── generate_data.py       # Synthetic data generation
│   ├── load_data.py          # ETL data loading
│   ├── analytics.py          # Business intelligence queries
│   └── db_connection.py      # Database utilities
├── data/                      # Generated CSV files
└── logs/                      # Application logs
```

## 🔧 Configuration

### Database Connection (.env)
```
DB_HOST=localhost
DB_PORT=5433
DB_NAME=warehouse
DB_USER=postgres
DB_PASSWORD=password
```

### Docker Services
- **PostgreSQL**: Port 5433 (host) → 5432 (container)
- **pgAdmin**: Port 8118 → 80 (web interface)

## 📊 Data Warehouse Metrics

- **Total Records**: ~91,000
- **Fact Table**: 85,000+ sales transactions
- **Time Dimension**: 2 years (2023-2024)
- **Customer Base**: 5,000 unique customers
- **Product Catalog**: 500 products across 5 categories
- **Database Size**: ~50MB

## 🎨 Business Intelligence Examples

### Key Performance Indicators
- Monthly revenue trends and growth rates
- Customer lifetime value by segment
- Product performance and profitability analysis
- Seasonal sales patterns and weekend vs weekday analysis

### Operational Analytics
- Inventory turnover by product category
- Customer acquisition and retention metrics
- Discount effectiveness analysis
- Geographic sales distribution

## 🚦 Performance Considerations

### Database Optimization
- Strategic indexing on foreign keys and frequently filtered columns
- Batch inserts for improved loading performance
- Connection pooling for concurrent access
- Query optimization using EXPLAIN ANALYZE

### Scalability Features
- Containerized architecture for easy horizontal scaling
- Modular Python scripts for pipeline extensibility
- Configurable batch sizes for memory management
- Separate dimension and fact table loading

## 🔍 Monitoring & Maintenance

### Health Checks
- Docker container health monitoring
- Database connection validation
- Data quality checks and constraints
- Performance metrics tracking

### Backup Strategy
- Docker volume persistence
- Regular database exports
- Version-controlled schema migrations
- Automated testing pipeline

## 💡 Future Enhancements

### Advanced Features
- [ ] Real-time streaming data ingestion
- [ ] Data lineage and cataloging
- [ ] Automated data quality monitoring
- [ ] Machine learning integration
- [ ] API endpoints for data access
- [ ] Dashboard visualization (Grafana/Tableau)

### Production Considerations
- [ ] Security hardening and authentication
- [ ] High availability and disaster recovery
- [ ] Data encryption at rest and in transit
- [ ] Automated backup and recovery procedures
- [ ] Performance monitoring and alerting
- [ ] CI/CD pipeline integration

## 📧 Contact

**Data Engineer Portfolio Project**  
Demonstrates: ETL, Data Warehousing, SQL, Python, Docker, PostgreSQL

*This project showcases practical data engineering skills including data modeling, ETL pipeline development, database optimization, and business intelligence analytics.*