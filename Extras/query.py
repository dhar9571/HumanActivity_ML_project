import psycopg2
from psycopg2 import sql
from main import prepare_data
import pandas as pd

df = pd.read_csv("C:\\Users\\dk957\\OneDrive\\Desktop\\HumanActivity_ML\\raw_files\\complete.csv")
# columns = df.columns.tolist()
columns = [f'"{column}"' for column in df.columns]
placeholders = ', '.join(['%s'] * len(columns))
insert_query = f'INSERT INTO sensor_data ({", ".join(columns)}) VALUES ({placeholders});'

# Prepare the data for batch insert
data_to_insert = [tuple(row) for row in df.values]

# Database connection parameters
db_params = {
    'dbname': 'humanActivity',
    'user': 'postgres',
    'password': 'Kabirsingh@123',
    'host': 'localhost',  # or your PostgreSQL server's host address
    'port': '5432'  # default port for PostgreSQL
}


# Establish a connection to the PostgreSQL database
try:
    conn = psycopg2.connect(**db_params)
    print("Connection successful")
except Exception as e:
    print(f"Error connecting to database: {e}")

# columns = prepare_data()

# Create a cursor object to interact with the database
cur = conn.cursor()

cur.execute("SET search_path TO master;")
# Execute the insert query for all rows
cur.executemany(insert_query, data_to_insert)

# Commit the transaction
conn.commit()
print("Data inserted successfully")

# Close the cursor and connection
cur.close()
conn.close()

# Set the search path to the 'master' schema
# cur.execute("SET search_path TO master;")

# Example query: Create a table
# create_table_query = '''
# create table sensor_data (abc float);'''

# cur.execute(create_table_query)
# print("table created successfully")

# Commit after creating the table
# conn.commit()


# try:
#     # cnt = 1
#     # for column in columns:
#     #     if cnt < 562:
#     #         cur.execute(f'alter table sensor_data add column "{column}" float;')
#     #         cnt += 1
#     #     elif cnt == 562:
#     #         cur.execute(f'alter table sensor_data add column "{column}" integer;')
#     #         cnt += 1
#     #     else:
#     #         cur.execute(f'alter table sensor_data add column "{column}" text;')
#     #         cnt += 1
    
#     cur.execute(f"insert into sensor_data select * from {df}")
        
#     # Commit after adding all columns
#     conn.commit()
#     print("Columns added successfully")
# except Exception as e:
#     print(f"Error executing query: {e}")