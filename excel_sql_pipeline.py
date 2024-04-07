import pandas as pd
import sql

table_name = "sales"


def row_to_string(row):
    result = f'''({row["id"]}, "{row["name"]}", "{row["email"]}", "{row["gender"]}",
                "{row["date"].date()}", {row["total"]}, "{row["location"]}"), \n'''
    return result

def create_insertion_string(df):
    query = f"INSERT INTO {table_name} VALUES \n"
    for index, record in df.iterrows():
        row_string = row_to_string(record)
        query += row_string
    query = ';'.join(query.rsplit(', \n', 1))
    return query

df = pd.read_csv("MOCK_DATA.csv")

# print(df.head())
# print(df.info())

# data processing
df['total'] = df['total'].replace("[$,]", "", regex=True).astype('float')
df['date'] = pd.to_datetime(df['date'])

print(df.head())
print(df.info())

# Connect to database
db_connection = sql.create_db_connection("localhost", "root", sql.get_password(), "sales")

# Insert Data
insertion_query = create_insertion_string(df)
#print(insertion_query[150:])

sql.execute_query(db_connection, insertion_query)

# Disconnect from database?
db_connection.disconnect()