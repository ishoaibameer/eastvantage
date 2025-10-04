# Eastvantage — Data Engineer Assignment Solution

This repository contains my solution for the **Eastvantage Data Engineer** technical assignment.  
It demonstrates two clean approaches for extracting and transforming customer purchase data using **SQL** and **Pandas**.

---

## Problem Statement
Given the provided SQLite database, calculate the **total quantity of each item purchased** by customers **aged 18–35**.

### Requirements
- Ignore `NULL` quantities.
- Exclude items with total quantity ≤ 0.
- Cast all quantities to integers.
- Output should be semicolon-delimited CSV files.

---

## Database Schema
| Table | Description | Key Columns |
|--------|--------------|--------------|
| `customers` | Customer demographics | `customer_id`, `age` |
| `sales` | Sales metadata | `sales_id`, `customer_id` |
| `orders` | Order line items | `order_id`, `sales_id`, `item_id`, `quantity` |
| `items` | Product catalog | `item_id`, `item_name` |

---

## Approach

### SQL Solution
A single SQL query joins all four tables, filters customers aged 18–35, sums up valid quantities, and exports results to `output_sql.csv`.

```sql
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
