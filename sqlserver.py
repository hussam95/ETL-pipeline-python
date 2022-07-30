from pyodbc import connect

# let's connect 
try:
    conn = connect('Driver={SQL Server};'
                    'Server=LAPTOP-57F3LA9L;'
                    'Database=zembuilders;'
                    'Trusted_Connection=yes;')
    
except Exception as e:
    print(e)

# cursor object to execute sql queries
cursor = conn.cursor()

# # create a new table in zembuilders db (one-time)

# cursor.execute('''
# 		CREATE TABLE admin (
# 			product_id int primary key,
# 			product_name nvarchar(50),
# 			price int
# 			)
#                ''')

# conn.commit()

# # Populate new table

# cursor.execute('''
#                 INSERT INTO admin (product_id, product_name, price)
#                 VALUES
#                 (5,'Chair',120),
#                 (6,'Tablet',300)
#                 ''')
# conn.commit()

# let's add more values
with cursor:
    cursor.execute('''
            INSERT INTO admin (product_id, product_name, price)
            VALUES
                (8,'Desktop Computer',800),
                (9,'Laptop',12000),
                (10,'Tablet',2000),
                (11,'Monitor',3500),
                (12,'Printer',1500)
                    ''')
    conn.commit()