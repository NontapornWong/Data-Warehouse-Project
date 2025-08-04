from faker import Faker
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import os

# Initialize Faker
fake = Faker()

def generate_date_dimension():
    """Generate date dimension for 2 years (2023-2024) - more efficient"""
    print("Generating date dimension...")
    
    start_date = datetime(2023, 1, 1)
    end_date = datetime(2024, 12, 31)
    
    # Use pandas date_range instead of manual loop
    date_range = pd.date_range(start=start_date, end=end_date, freq='D')
    
    dates_data = {
        'date_value': date_range.date,
        'year': date_range.year,
        'quarter': date_range.quarter,
        'month': date_range.month,
        'day': date_range.day,
        'day_of_week': date_range.dayofweek + 1,  # Monday=1, Sunday=7
        'week_of_year': date_range.isocalendar().week,
        'is_weekend': date_range.dayofweek >= 5
    }
    
    df = pd.DataFrame(dates_data)
    df.to_csv('data/date_dimension.csv', index=False)
    print(f"✅ Generated {len(df)} date records")
    return df

def generate_customers(num_customers=5000):  # Reduced from 10k to 5k
    """Generate customer dimension"""
    print(f"Generating {num_customers} customers...")
    
    # Generate in batches to save memory
    batch_size = 1000
    customers_list = []
    customer_segments = ['Premium', 'Standard', 'Basic', 'VIP']
    
    for batch in range(0, num_customers, batch_size):
        batch_customers = []
        batch_end = min(batch + batch_size, num_customers)
        
        for i in range(batch, batch_end):
            batch_customers.append({
                'first_name': fake.first_name(),
                'last_name': fake.last_name(),
                'email': fake.email(),
                'phone': fake.phone_number()[:20],  # Limit length
                'city': fake.city(),
                'state': fake.state_abbr(),  # Use abbreviation
                'country': 'USA',  # Simplify to reduce memory
                'customer_segment': np.random.choice(customer_segments, p=[0.1, 0.4, 0.4, 0.1]),
                'registration_date': fake.date_between(start_date='-1y', end_date='today')
            })
        
        customers_list.extend(batch_customers)
        print(f"  Generated batch {batch//batch_size + 1}/{(num_customers-1)//batch_size + 1}")
    
    df = pd.DataFrame(customers_list)
    df.to_csv('data/customers.csv', index=False)
    print(f"✅ Generated {len(df)} customer records")
    return df

def generate_products(num_products=500):  # Reduced from 1k to 500
    """Generate product dimension"""
    print(f"Generating {num_products} products...")
    
    categories = {
        'Electronics': ['Smartphones', 'Laptops', 'Headphones'],
        'Clothing': ['Shirts', 'Pants', 'Shoes'],
        'Home': ['Furniture', 'Appliances', 'Decor'],
        'Books': ['Fiction', 'Non-Fiction', 'Educational'],
        'Sports': ['Equipment', 'Apparel', 'Accessories']
    }
    
    brands = ['BrandA', 'BrandB', 'BrandC', 'Generic']
    
    products = []
    for i in range(num_products):
        category = np.random.choice(list(categories.keys()))
        subcategory = np.random.choice(categories[category])
        brand = np.random.choice(brands)
        
        cost = round(np.random.uniform(10, 200), 2)
        price = round(cost * np.random.uniform(1.5, 2.5), 2)
        
        products.append({
            'product_name': f"{brand} {subcategory} {i+1}",
            'category': category,
            'subcategory': subcategory,
            'brand': brand,
            'price': price,
            'cost': cost
        })
    
    df = pd.DataFrame(products)
    df.to_csv('data/products.csv', index=False)
    print(f"✅ Generated {len(df)} product records")
    return df

if __name__ == "__main__":
    os.makedirs('data', exist_ok=True)
    
    print("🚀 Starting data generation...")
    
    dates_df = generate_date_dimension()
    customers_df = generate_customers()
    products_df = generate_products()
    
    print("\n📊 Data generation summary:")
    print(f"- Dates: {len(dates_df)} records")
    print(f"- Customers: {len(customers_df)} records") 
    print(f"- Products: {len(products_df)} records")
    print("\n✅ All dimension data generated successfully!")