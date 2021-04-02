import sqlite3

conn = sqlite3.connect('shootingstarschema.sqlite')
cur = conn.cursor()

cur.executescript('''
DROP TABLE IF EXISTS Region;
DROP TABLE IF EXISTS Nation;
DROP TABLE IF EXISTS Customer;
DROP TABLE IF EXISTS Supplier;
DROP TABLE IF EXISTS Part;
DROP TABLE IF EXISTS Orders;
DROP TABLE IF EXISTS Partsupp;
DROP TABLE IF EXISTS Lineitem;

CREATE TABLE Region (
    r_regionkey  INTEGER NOT NULL PRIMARY KEY,
    r_name    TEXT UNIQUE,
    r_comment    TEXT
);

CREATE TABLE Part (
    p_partkey  INTEGER NOT NULL PRIMARY KEY,
    p_name    TEXT UNIQUE,
    p_mfgr    TEXT,
    p_brand    TEXT,
    p_type    TEXT,
    p_size    TEXT,
    p_container    TEXT,
    p_retailprice    TEXT ,
    p_comment    TEXT
);

CREATE TABLE Nation (
    n_nationkey  INTEGER NOT NULL PRIMARY KEY,
    n_name    TEXT UNIQUE,
    n_regionkey    INTEGER,
    n_comment    TEXT,
    FOREIGN KEY (n_regionkey)
    REFERENCES Region (r_regionkey)
);

CREATE TABLE Supplier (
    s_suppkey  INTEGER NOT NULL PRIMARY KEY,
    s_name    TEXT UNIQUE,
    s_address    TEXT,
    s_nationkey    INTEGER,
    s_phone    TEXT UNIQUE,
    s_acctbal    TEXT,
    s_comment    TEXT,
    FOREIGN KEY (s_nationkey)
    REFERENCES Nation (n_nationkey)
);

CREATE TABLE Customer (
    c_custkey  INTEGER NOT NULL PRIMARY KEY,
    c_name    TEXT UNIQUE,
    c_address    TEXT,
    c_nationkey    INTEGER,
    c_phone    TEXT UNIQUE,
    c_acctbal    FLOAT,
    c_mktsegment    TEXT,
    c_comment    TEXT,
    FOREIGN KEY (c_nationkey)
    REFERENCES Nation (n_nationkey)
);

CREATE TABLE Partsupp (
    ps_id  INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT UNIQUE,
    ps_partkey  INTEGER,
    ps_suppkey  INTEGER,
    ps_availqty    TEXT,
    ps_supplycost  TEXT,
    ps_comment    TEXT,
    FOREIGN KEY (ps_partkey)
    REFERENCES Part (p_partkey)
    FOREIGN KEY (ps_suppkey)
    REFERENCES Supplier (s_suppkey)
                    
);

CREATE TABLE Orders (
    o_orderkey  INTEGER NOT NULL PRIMARY KEY,
    o_custkey  INTEGER,
    o_orderstatus  TEXT,
    o_totalprice  FLOAT,
    o_orderdate  DATE,
    o_orderpriority  TEXT,
    o_cleark  TEXT,
    o_shippriority  TEXT,
    o_comment TEXT,
    FOREIGN KEY (o_custkey)
    REFERENCES Customer (c_custkey)
);


CREATE TABLE Lineitem (
    l_id  INTEGER NOT NULL PRIMARY KEY,
    l_orderkey  INTEGER,
    l_ps_id  INTEGER,
    l_linenumber  TEXT,
    l_quantity  FLOAT,
    l_extendedprice  TEXT,
    l_discount  TEXT,
    l_tax  TEXT,
    l_returnflag  TEXT,
    l_linestatus  TEXT,
    l_shipdate  DATE,
    l_commitdate  DATE,
    l_receiptdata  DATE,
    l_shipinstruct  TEXT,
    l_shipmode  TEXT,
    l_comment  TEXT,
    FOREIGN KEY (l_orderkey)
    REFERENCES Orders (o_orderkey)
    FOREIGN KEY (l_ps_id)
    REFERENCES Partsupp (ps_id)
);

''')

lh = open('lineitem.tbl')
for line in lh:
    entry = line.split("|") 
    
    l_id= entry[0] 
    l_orderkey= entry[1] 
    l_ps_id= entry[2] 
    l_linenumber= entry[3] 
    l_quantity= entry[4] 
    l_extendedprice = entry[5]   
    l_discount= entry[6] 
    l_tax= entry[7] 
    l_returnflag= entry[8] 
    l_linestatus= entry[9] 
    l_shipdate= entry[10] 
    l_commitdate= entry[11] 
    l_receiptdata= entry[12] 
    l_shipinstruct= entry[13] 
    l_shipmode= entry[14] 
    l_comment= entry[15] 
    
    cur.execute('''INSERT OR IGNORE INTO Lineitem (    l_id,    l_orderkey,
    l_ps_id,l_linenumber,    l_quantity, l_extendedprice,   
    l_discount, l_tax, l_returnflag, l_linestatus,
    l_shipdate,l_commitdate, l_receiptdata,l_shipinstruct ,
    l_shipmode ,    l_comment ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)''',
    (    l_id,    l_orderkey,
    l_ps_id,l_linenumber,    l_quantity, l_extendedprice,   
    l_discount, l_tax, l_returnflag, l_linestatus,
    l_shipdate,l_commitdate, l_receiptdata,l_shipinstruct ,
    l_shipmode ,    l_comment ))
conn.commit()

psh = open('partsupp.tbl')
for line in psh:
    entry = line.split("|") 
    
    ps_partkey= entry[0]
    ps_suppkey= entry[1]    
    ps_availqty = entry[2]
    ps_supplycost = entry[3]    
    ps_comment= entry[4]
        
    cur.execute('''INSERT OR IGNORE INTO Partsupp ( ps_partkey,    ps_suppkey,    ps_availqty ,
    ps_supplycost ,    ps_comment) VALUES (?,?,?,?,?)''',
    (ps_partkey,    ps_suppkey,    ps_availqty ,
    ps_supplycost ,    ps_comment))
conn.commit()

oh = open('orders.tbl')
for line in oh:
    entry = line.split("|") 
    
    o_orderkey= entry[0]
    o_custkey= entry[1]
    o_orderstatus= entry[2]
    o_totalprice= entry[3] 
    o_orderdate= entry[4]
    o_orderpriority= entry[5]
    o_cleark= entry[6]
    o_shippriority= entry[7]
    o_comment= entry[8]
    
    cur.execute('''INSERT OR IGNORE INTO Orders (o_orderkey,    o_custkey,    o_orderstatus,
    o_totalprice,    o_orderdate,    o_orderpriority,
    o_cleark,    o_shippriority,    o_comment) VALUES (?,?,?,?,?,?,?,?,?)''',
    (   o_orderkey,    o_custkey,    o_orderstatus,
    o_totalprice,    o_orderdate,    o_orderpriority,
    o_cleark,    o_shippriority,    o_comment))
    
conn.commit()

ph = open('part.tbl')
for line in ph:
    entry = line.split("|")  
    
    p_partkey = entry[0]
    p_name = entry[1]
    p_mfgr = entry[2]
    p_brand = entry[3]
    p_type = entry[4]
    p_size = entry[5]
    p_container = entry[6]
    p_retailprice = entry[7]
    p_comment = entry[8]
    
    cur.execute('''INSERT OR IGNORE INTO Part (
    p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment )  VALUES (?,?,?,?,?,?,?,?,?)''',
    ( p_partkey,
    p_name,
    p_mfgr,
    p_brand,
    p_type,
    p_size,
    p_container,
    p_retailprice,
    p_comment ))

conn.commit()

sh = open('supplier.tbl')
for line in sh:
    entry = line.split("|")  
    
    s_suppkey = entry[0]
    s_name = entry[1]
    s_address = entry[2]
    s_nationkey = entry[3]
    s_phone = entry[4]
    s_acctbal = entry[5]
    s_comment = entry[6]
    
    cur.execute('''INSERT OR IGNORE INTO Supplier (
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal,
    s_comment) VALUES (?,?,?,?,?,?,?)''',(s_suppkey,
    s_name,s_address,    s_nationkey,    s_phone,    s_acctbal,
    s_comment))
conn.commit()

ch = open('customer.tbl')
for line in ch:
    entry = line.split("|")
    
    c_custkey = entry[0]
    c_name = entry[1]
    c_address = entry[2]
    c_nationkey = entry[3]    
    c_phone = entry[4]
    c_acctbal = entry[5]
    c_mktsegment = entry[6]
    c_comment = entry[7]
    
    cur.execute('''INSERT OR IGNORE INTO Customer (c_custkey, c_name,
    c_address, c_nationkey,c_phone,
    c_acctbal,c_mktsegment, c_comment)
    VALUES ( ?, ?, ?, ?, ?, ?, ?, ?)''', (c_custkey, c_name,
    c_address, c_nationkey,c_phone,
    c_acctbal,c_mktsegment, c_comment))
conn.commit()

nh = open('nation.tbl')
for line in nh:
    entry = line.split("|")
    
    n_nationkey = entry[0]
    n_name = entry[1]
    n_regionkey = entry[2]
    n_comment = entry[3]
        
    cur.execute('''INSERT OR IGNORE INTO Nation (n_nationkey,
    n_name,n_regionkey, n_comment)
    VALUES ( ?, ?, ?, ?)''', (n_nationkey, n_name,n_regionkey, n_comment))
conn.commit()  

rh = open('region.tbl')
for line in rh:
    entry = line.split("|")
    
    r_regionkey = entry[0]
    r_name = entry[1]
    r_comment = entry[2]

    cur.execute('''INSERT OR IGNORE INTO Region (r_regionkey, r_name, r_comment)
        VALUES ( ?, ?, ? )''', (r_regionkey, r_name, r_comment) )
conn.commit()
    

cur.execute('''ALTER TABLE Customer ADD COLUMN c_richness INTEGER''')
cur.execute('''UPDATE Customer SET c_richness=
CASE WHEN c_acctbal >1000 THEN 1
WHEN c_acctbal <0 THEN -1
ELSE 0  END''')
conn.commit()

cur.execute('''ALTER TABLE Lineitem ADD COLUMN l_revenue FLOAT''')
cur.execute('''UPDATE Lineitem SET l_revenue=l_quantity *l_extendedprice''')
conn.commit()
