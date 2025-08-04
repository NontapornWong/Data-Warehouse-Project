import psycopg2
import pandas as pd
import numpy as np
from dotenv import load_dotenv
import os
from datetime import datetime

load_dotenv()

class DataLoader:
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
    
    def load_dimension_tables(self):
        """Load dimension tables from CSV files"""
        print("🔄 Loading dimension tables...")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Load date dimension
            print("Loading dates...")
            dates_df = pd.read_csv('data/date_dimension.csv')
            for _, row in dates_df.iterrows():
                cursor.execute("""
                    INSERT INTO date_dimension (date_value, year, quarter, month, day, day_of_week, week_of_year, is_weekend)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
            print(f"✅ Loaded {len(dates_df)} dates")
            
            # Load customers
            print("Loading customers...")
            customers_df = pd.read_csv('data/customers.csv')
            for _, row in customers_df.iterrows():
                cursor.execute("""
                    INSERT INTO customers (first_name, last_name, email, phone, city, state, country, customer_segment, registration_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, tuple(row))
            print(f"✅ Loaded {len(customers_df)} customers")
            
            # Load products
            print("Loading products...")
            products_df = pd.read_csv('data/products.csv')
            for _, row in products_df.iterrows():
                cursor.execute("""
                    INSERT INTO products (product_name, category, subcategory, brand, price, cost)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, tuple(row))
            print(f"✅ Loaded {len(products_df)} products")
            
            conn.commit()
    
    def generate_and_load_transactions(self, num_transactions=85000):
        """Generate and load sales transactions"""
        print(f"🔄 Generating {num_transactions} sales transactions...")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get dimension IDs
            cursor.execute("SELECT customer_id FROM customers")
            customer_ids = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT product_id FROM products")
            product_ids = [row[0] for row in cursor.fetchall()]
            
            cursor.execute("SELECT date_id FROM date_dimension")
            date_ids = [row[0] for row in cursor.fetchall()]
            
            print(f"Available: {len(customer_ids)} customers, {len(product_ids)} products, {len(date_ids)} dates")
            
            # Generate transactions in batches
            batch_size = 5000
            for batch in range(0, num_transactions, batch_size):
                batch_end = min(batch + batch_size, num_transactions)
                batch_transactions = []
                
                for i in range(batch, batch_end):
                    customer_id = int(np.random.choice(customer_ids))
                    product_id = int(np.random.choice(product_ids))
                    date_id = int(np.random.choice(date_ids))
                    
                    # Get product price
                    cursor.execute("SELECT price FROM products WHERE product_id = %s", (product_id,))
                    unit_price = cursor.fetchone()[0]
                    
                    quantity = np.random.randint(1, 6)  # 1-5 items
                    discount_pct = np.random.choice([0, 0.05, 0.10, 0.15], p=[0.7, 0.15, 0.10, 0.05])
                    
                    total_before_discount = float(unit_price) * quantity
                    discount_amount = round(total_before_discount * discount_pct, 2)
                    total_amount = round(total_before_discount - discount_amount, 2)
                    
                    batch_transactions.append((
                        customer_id, product_id, date_id, quantity, 
                        float(unit_price), total_amount, discount_amount
                    ))
                
                # Bulk insert batch
                cursor.executemany("""
                    INSERT INTO sales_transactions (customer_id, product_id, date_id, quantity, unit_price, total_amount, discount_amount)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                """, batch_transactions)
                
                conn.commit()
                print(f"  Loaded batch {batch//batch_size + 1}/{(num_transactions-1)//batch_size + 1}")
            
            print(f"✅ Generated and loaded {num_transactions} transactions")
    
    def verify_data_load(self):
        """Verify all data was loaded correctly"""
        print("\n🔍 Verifying data load...")
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            tables = ['customers', 'products', 'date_dimension', 'sales_transactions']
            
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                print(f"✅ {table}: {count:,} records")
            
            # Sample data check
            cursor.execute("""
                SELECT c.first_name, p.product_name, d.date_value, st.total_amount
                FROM sales_transactions st
                JOIN customers c ON st.customer_id = c.customer_id
                JOIN products p ON st.product_id = p.product_id  
                JOIN date_dimension d ON st.date_id = d.date_id
                LIMIT 5
            """)
            
            print("\n Sample transactions:")
            for row in cursor.fetchall():
                print(f"  {row[0]} bought {row[1]} on {row[2]} for ${row[3]}")


def main():
    print("🚀 Starting data loading process...")
    
    loader = DataLoader()
    
    try:        
        # Load dimension tables
        loader.load_dimension_tables()
        
        # Generate and load fact table
        loader.generate_and_load_transactions()
        
        # Verify everything loaded correctly
        loader.verify_data_load()
        
        print("\n🎉 Data loading completed successfully!")
        print("💾 Your data warehouse now contains ~100k records total")
        
    except Exception as e:
        print(f"❌ Error during data loading: {e}")

if __name__ == "__main__":
    main()