#!/usr/bin/env python3
"""
Eastvantage - Data Engineer assignment solution

This script runs two approaches to compute total quantities of each item bought
per customer aged 18–35, excluding items with zero totals and ignoring NULL quantities.
Outputs are semicolon-delimited CSV files.

Tables expected in the SQLite DB:
- customers(customer_id, age)
- sales(sales_id, customer_id)
- orders(order_id, sales_id, item_id, quantity)
- items(item_id, item_name)
"""

import sqlite3, argparse, pandas as pd

SQL_QUERY = """
SELECT c.customer_id AS Customer,
       c.age AS Age,
       it.item_name AS Item,
       CAST(SUM(o.quantity) AS INTEGER) AS Quantity
FROM customers c
JOIN sales s ON s.customer_id = c.customer_id
JOIN orders o ON o.sales_id = s.sales_id
JOIN items it ON it.item_id = o.item_id
WHERE c.age BETWEEN 18 AND 35
  AND o.quantity IS NOT NULL
GROUP BY c.customer_id, c.age, it.item_name
HAVING SUM(o.quantity) > 0
ORDER BY c.customer_id, it.item_name;
"""

def sql_solution(db_path, out_csv):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(SQL_QUERY, conn)
    df.to_csv(out_csv, sep=';', index=False, lineterminator='\n')
    conn.close()
    print(f"✅ SQL result written -> {out_csv}")

def pandas_solution(db_path, out_csv):
    conn = sqlite3.connect(db_path)
    customers = pd.read_sql_query("SELECT * FROM customers;", conn)
    sales = pd.read_sql_query("SELECT * FROM sales;", conn)
    orders = pd.read_sql_query("SELECT * FROM orders;", conn)
    items = pd.read_sql_query("SELECT * FROM items;", conn)

    df = (orders
          .merge(sales, on='sales_id')
          .merge(customers, on='customer_id')
          .merge(items, on='item_id'))

    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
    df2 = df[df['quantity'].notna() & df['age'].between(18,35)]

    agg = (df2.groupby(['customer_id','age','item_name'], dropna=False)['quantity']
           .sum()
           .reset_index()
           .rename(columns={'customer_id':'Customer','age':'Age',
                            'item_name':'Item','quantity':'Quantity'}))

    agg = agg[agg['Quantity'] > 0].sort_values(['Customer','Item'])
    agg['Quantity'] = agg['Quantity'].astype(int)
    agg.to_csv(out_csv, sep=';', index=False, lineterminator='\n')
    conn.close()
    print(f"✅ Pandas result written -> {out_csv}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--db", required=True)
    parser.add_argument("--out-sql", default="output_sql.csv")
    parser.add_argument("--out-pandas", default="output_pandas.csv")
    args = parser.parse_args()

    sql_solution(args.db, args.out_sql)
    pandas_solution(args.db, args.out_pandas)
