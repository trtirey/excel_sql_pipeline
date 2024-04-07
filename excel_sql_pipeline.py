import pandas as pd
import sql

table_name = "sales"

def row_to_string(row):
    result = f'''({row["id"]}, '{row["name"]}', '{row["email"]}', '{row["gender"]}',
                '{row["date"]}', {row["total"]}, '{row["location"]}'), \n'''
    return result

def create_insertion_string(df):
    query = f"INSERT INTO {table_name} VALUES \n"
    for index, record in df.iterrows():
        row_string = row_to_string(record)
        query += row_string
    return query

df = pd.read_csv("MOCK_DATA.csv")

# print(df.head())
# print(df.info())

df['total'] = df['total'].replace("[$,]", "", regex=True).astype('float')

print(df.head())
print(df.info())

# Any other data processing
# Convert Date to 'YYYY-MM-DD' format

# Connect to database
db_connection = sql.create_db_connection("localhost", "root", sql.get_password(), "sales")

# Insert Data
insertion_query = create_insertion_string(df)
print(insertion_query[:150])

db_connection._execute_query(insertion_query)

# Disconnect from database?
db_connection.disconnect()