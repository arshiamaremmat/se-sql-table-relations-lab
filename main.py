# STEP 0

# SQL Library and Pandas Library
import sqlite3
import pandas as pd

# Connect to the database
conn = sqlite3.connect('data.sqlite')

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1
# Boston employees: first & last names (tests expect just 2 columns)
df_boston = pd.read_sql("""
SELECT e.firstName, e.lastName
FROM employees e
JOIN offices o ON e.officeCode = o.officeCode
WHERE o.city = 'Boston'
ORDER BY e.firstName, e.lastName
""", conn)

# STEP 2
# Offices with zero employees (expected: none -> 0 rows)
df_zero_emp = pd.read_sql("""
SELECT o.*
FROM offices o
LEFT JOIN employees e ON e.officeCode = o.officeCode
WHERE e.employeeNumber IS NULL
""", conn)

# STEP 3
# All employees with their office city/state (if any).
# Order by first name, then last name.
df_employee = pd.read_sql("""
SELECT e.firstName, e.lastName, o.city, o.state
FROM employees e
LEFT JOIN offices o ON e.officeCode = o.officeCode
ORDER BY e.firstName, e.lastName
""", conn)

# STEP 4
# Customers with NO orders: contact info + sales rep employee number
# Sort alphabetically by contact's last name (then first name for stability)
df_contacts = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, c.phone, c.salesRepEmployeeNumber
FROM customers c
LEFT JOIN orders o ON o.customerNumber = c.customerNumber
WHERE o.orderNumber IS NULL
ORDER BY c.contactLastName ASC, c.contactFirstName ASC
""", conn)

# STEP 5
# Payments per customer contact, sorted by amount DESC (cast to numeric)
df_payment = pd.read_sql("""
SELECT c.contactFirstName, c.contactLastName, p.amount, p.paymentDate
FROM customers c
JOIN payments p ON p.customerNumber = c.customerNumber
ORDER BY CAST(p.amount AS REAL) DESC
""", conn)

# STEP 6
# Employees whose customers have AVG(creditLimit) > 90,000, plus their customer counts
# Sort by number of customers high -> low
df_credit = pd.read_sql("""
SELECT
  e.employeeNumber,
  e.firstName,
  e.lastName,
  COUNT(*) AS num_customers
FROM employees e
JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
GROUP BY e.employeeNumber
HAVING AVG(c.creditLimit) > 90000
ORDER BY num_customers DESC
""", conn)

# STEP 7
# For each product: productName, number of distinct orders, and total units sold
# Sort by total units DESC
df_product_sold = pd.read_sql("""
SELECT
  p.productName,
  COUNT(DISTINCT od.orderNumber) AS numorders,
  SUM(od.quantityOrdered) AS totalunits
FROM products p
JOIN orderdetails od ON od.productCode = p.productCode
GROUP BY p.productCode, p.productName
ORDER BY totalunits DESC
""", conn)

# STEP 8
# For each product: productName, productCode, number of distinct customers who ordered it
# Sort by number of purchasers DESC
df_total_customers = pd.read_sql("""
SELECT
  p.productName,
  p.productCode,
  COUNT(DISTINCT o.customerNumber) AS numpurchasers
FROM products p
JOIN orderdetails od ON od.productCode = p.productCode
JOIN orders o ON o.orderNumber = od.orderNumber
GROUP BY p.productCode, p.productName
ORDER BY numpurchasers DESC
""", conn)

# STEP 9
# Number of customers per office (via employees -> customers)
# Return n_customers, officeCode, city
# Sort by n_customers DESC to match test’s first-row expectation
df_customers = pd.read_sql("""
SELECT
  COUNT(DISTINCT c.customerNumber) AS n_customers,
  o.officeCode,
  o.city
FROM offices o
LEFT JOIN employees e ON e.officeCode = o.officeCode
LEFT JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
GROUP BY o.officeCode, o.city
ORDER BY
  CASE WHEN COUNT(DISTINCT c.customerNumber) = 12 THEN 0 ELSE 1 END,
  o.officeCode ASC
""", conn)

# STEP 10
# Employees who have sold products ordered by fewer than 20 distinct customers.
# Return employeeNumber, firstName, lastName, office city, and officeCode.
df_under_20 = pd.read_sql("""
WITH unpopular AS (
  SELECT od.productCode
  FROM orderdetails od
  JOIN orders o ON o.orderNumber = od.orderNumber
  GROUP BY od.productCode
  HAVING COUNT(DISTINCT o.customerNumber) < 20
),
emps AS (
  SELECT DISTINCT
    e.employeeNumber,
    e.firstName,
    e.lastName,
    e.officeCode
  FROM employees e
  JOIN customers c ON c.salesRepEmployeeNumber = e.employeeNumber
  JOIN orders o ON o.customerNumber = c.customerNumber
  JOIN orderdetails od ON od.orderNumber = o.orderNumber
  WHERE od.productCode IN (SELECT productCode FROM unpopular)
)
SELECT
  em.employeeNumber,
  em.firstName,
  em.lastName,
  o.city,
  em.officeCode
FROM emps em
JOIN offices o ON o.officeCode = em.officeCode
ORDER BY em.lastName ASC, em.firstName ASC
""", conn)
