-----TASK 5-----

---What are the bottom 3 nations in terms of revenue?---

SELECT Orders.o_totalprice, Nation.n_name
FROM Orders
LEFT JOIN Customer ON Orders.o_custkey=Customer.c_custkey
LEFT JOIN Nation ON Nation.n_nationkey = Customer.c_nationkey
GROUP BY Nation.n_name
ORDER BY Orders.o_totalprice ASC LIMIT 3;

------------------------------------------------------------------------------------
---From the top 3 nations, what is the most common shipping mode?---

SELECT COUNT(Lineitem.l_shipmode), Lineitem.l_shipmode FROM (SELECT SUM(Orders.o_totalprice), Nation.n_name, Orders.o_orderkey, Orders.o_custkey, Customer.c_nationkey
	FROM Orders
	LEFT JOIN Customer ON Orders.o_custkey=Customer.c_custkey
	LEFT JOIN Nation ON Nation.n_nationkey = Customer.c_nationkey
	GROUP BY Nation.n_name
	ORDER BY SUM(Orders.o_totalprice) DESC LIMIT 3)
	AS TOPNATIONS
LEFT JOIN Lineitem ON Lineitem.l_orderkey=TOPNATIONS.o_orderkey
GROUP BY l_shipmode
ORDER BY COUNT(l_shipmode) DESC;

------------------------------------------------------------------------------------
---What are the top 5 selling months?---

SELECT SUM(o_totalprice), strftime('%m',Orders.o_orderdate) as o_month FROM ORDERS
GROUP BY o_month
ORDER BY SUM(o_totalprice) DESC LIMIT 5;

------------------------------------------------------------------------------------
---Who are the top customer(s) in terms of either revenue or quantity?---

---revenue:---
SELECT SUM(Orders.o_totalprice), Customer.c_name FROM Customer
JOIN Orders ON Customer.c_custkey=Orders.o_custkey
GROUP BY c_name
ORDER BY SUM(Orders.o_totalprice) DESC LIMIT 5;

---quantity:---
SELECT SUM(Lineitem.l_quantity),  Customer.c_name FROM Customer
JOIN Orders ON Customer.c_custkey=Orders.o_custkey
JOIN Lineitem ON Orders.o_orderkey=Lineitem.l_orderkey
GROUP BY c_name
ORDER BY SUM(l_quantity) DESC LIMIT 5;

------------------------------------------------------------------------------------
---Compare the sales revenue on a financial year-to-year (01 July to 30 June) basis.---

SELECT strftime('%Y', Orders.o_orderdate, '-6 months') AS FISCAL_YEAR, 
strftime('%Y %m', Orders.o_orderdate) AS REAL_DATE,
SUM(o_totalprice) FROM ORDERS
GROUP BY FISCAL_YEAR
ORDER BY SUM(o_totalprice) DESC;

