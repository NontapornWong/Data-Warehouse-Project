-- Performance indexes for common queries
CREATE INDEX idx_sales_customer_id ON sales_transactions(customer_id);
CREATE INDEX idx_sales_product_id ON sales_transactions(product_id);
CREATE INDEX idx_sales_date_id ON sales_transactions(date_id);
CREATE INDEX idx_sales_total_amount ON sales_transactions(total_amount);
CREATE INDEX idx_date_year_month ON date_dimension(year, month);
CREATE INDEX idx_customer_segment ON customers(customer_segment);
CREATE INDEX idx_product_category ON products(category);