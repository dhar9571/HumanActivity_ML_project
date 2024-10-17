from sqlalchemy import create_engine
import pandas as pd

# Database connection parameters
db_params = {
    'database': 'humanActivity',
    'user': 'postgres',
    'password': 'Kabirsingh@123',
    'host': 'localhost',  # or your PostgreSQL server's host address
    'port': '5432'  # default port for PostgreSQL
}

# Create a database connection string
connection_string = f"postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}"
print(connection_string)
engine = create_engine(connection_string)
print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

df = pd.read_csv("C:\\Users\\dk957\\OneDrive\\Desktop\\HumanActivity_ML\\raw_files\\complete.csv")

# Insert the DataFrame into the PostgreSQL table
print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
df.to_sql('sensor_data', engine, if_exists='append', index=False)
print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$")
print("Data inserted successfully")