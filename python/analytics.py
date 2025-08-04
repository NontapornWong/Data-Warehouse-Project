import psycopg2
import pandas as pd
from dotenv import load_dotenv
import os

load_dotenv()

class DataWarehouseAnalytics:
    def __init__(self):
        self.conn_params = {
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD')
        }

    def get_connection(self):
        return psycopg2.connect(**self.conn_params)
    
    def sales_by_month(self):
        """Monthly sales analysis"""
        print("Monthly Sales Analysis")
        print("="*50) 

        query = """ 
        SELECT 
            d.year,
            d.month,
            COUNT(*) as total_transactions,
            SUM(st.total_amount) as total_revenue,
            AVG(st.total_amount) as avg_order_value,
            SUM(st.quantity) as total_items_sold
        FROM sales_transactions st
        JOIN date_dimension d ON st.date_id = d.date_id
        GROUP BY d.year, d.month
        ORDER BY d.year, d.month;
        """

        df = pd.read_sql(query, self.get_connection())
        print(df.to_string(index=False))

    def top_customers(self, limit=10):
        """Top customers by revenue"""
        print(f"\n Top {limit} Customer by Revenue")
        print("="*50)

        query = """
        SELECT 
            c.first_name || ' ' || c.last_name as customer_name,
            c.customer_segment,
            c.city,
            COUNT(*) as total_orders,
            SUM(st.total_amount) as total_spent,
            AVG(st.total_amount) as avg_order_value
        FROM sales_transactions st
        JOIN customers c ON st.customer_id = c.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name, c.customer_segment, c.city
        ORDER BY total_spent DESC
        LIMIT %s;
        """

        df = pd.read_sql(query, self.get_connection(), params=[limit])
        print(df.to_string(index=False))
        return df
    
    def product_performance(self, limit=10):
        """Top performing products"""
        print(f"\n Top {limit} Products by Revenue")
        print("="*50)

        query = """
        SELECT 
            p.product_name,
            p.category,
            p.brand,
            COUNT(*) as times_sold,
            SUM(st.quantity) as total_quantity_sold,
            SUM(st.total_amount) as total_revenue,
            ROUND(AVG(st.total_amount), 2) as avg_sale_value
        FROM sales_transactions st
        JOIN products p ON st.product_id = p.product_id
        GROUP BY p.product_id, p.product_name, p.category, p.brand
        ORDER BY total_revenue DESC
        LIMIT %s;
        """
        df = pd.read_sql(query, self.get_connection(), params=[limit])
        print(df.to_string(index=False))
        return df
    
    def sales_by_category(self):
        """Sales performance by product category"""
        print("\n Sales by Product Category")
        print("=" * 50)
        
        query = """
        SELECT 
            p.category,
            COUNT(*) as total_transactions,
            SUM(st.quantity) as total_items_sold,
            SUM(st.total_amount) as total_revenue,
            ROUND(AVG(st.total_amount), 2) as avg_transaction_value,
            ROUND(SUM(st.total_amount) * 100.0 / SUM(SUM(st.total_amount)) OVER(), 2) as revenue_percentage
        FROM sales_transactions st
        JOIN products p ON st.product_id = p.product_id
        GROUP BY p.category
        ORDER BY total_revenue DESC;
        """
        
        df = pd.read_sql(query, self.get_connection())
        print(df.to_string(index=False))
        return df
    
    def customer_segment_analysis(self):
        """Customer segment performance"""
        print("\n Customer Segment Analysis")
        print("=" * 50)
        
        query = """
        SELECT 
            c.customer_segment,
            COUNT(DISTINCT c.customer_id) as total_customers,
            COUNT(*) as total_transactions,
            SUM(st.total_amount) as total_revenue,
            ROUND(AVG(st.total_amount), 2) as avg_order_value,
            ROUND(SUM(st.total_amount) / COUNT(DISTINCT c.customer_id), 2) as revenue_per_customer
        FROM sales_transactions st
        JOIN customers c ON st.customer_id = c.customer_id
        GROUP BY c.customer_segment
        ORDER BY total_revenue DESC;
        """
        
        df = pd.read_sql(query, self.get_connection())
        print(df.to_string(index=False))
        return df
    
    def weekend_vs_weekday_sales(self):
        """Weekend vs weekday sales comparison"""
        print("\n Weekend vs Weekday Sales")
        print("=" * 50)
        
        query = """
        SELECT 
            CASE WHEN d.is_weekend THEN 'Weekend' ELSE 'Weekday' END as day_type,
            COUNT(*) as total_transactions,
            SUM(st.total_amount) as total_revenue,
            ROUND(AVG(st.total_amount), 2) as avg_transaction_value
        FROM sales_transactions st
        JOIN date_dimension d ON st.date_id = d.date_id
        GROUP BY d.is_weekend
        ORDER BY total_revenue DESC;
        """
        
        df = pd.read_sql(query, self.get_connection())
        print(df.to_string(index=False))
        return df
    
def main():
    print("🏢 Data Warehouse Analytics Dashboard")
    print("=" * 60)
    
    analytics = DataWarehouseAnalytics()
    
    try:
        # Run all analytics
        analytics.sales_by_month()
        analytics.top_customers()
        analytics.product_performance()
        analytics.sales_by_category()
        analytics.customer_segment_analysis()
        analytics.weekend_vs_weekday_sales()
        
        print("\n✅ Analytics completed successfully!")
        print("💡 This demonstrates typical data warehouse queries for business intelligence")
        
    except Exception as e:
        print(f"❌ Error running analytics: {e}")

if __name__ == "__main__":
    main()