import pandas as pd
import sql
import sys

## Name of table to insert into
table_name = "sales"

# Consider implementing argument checking
file_to_convert = sys.argv[1]

# Convert a row, as returned from df.iterrows(), into an record string for a sql insert statement
def row_to_string(row):
    result = f'''({row["id"]}, "{row["name"]}", "{row["email"]}", "{row["gender"]}",
                "{row["date"].date()}", {row["total"]}, "{row["location"]}"), \n'''
    return result

# Build an return a sql insert statement
def create_insertion_string(df):
    query = f"INSERT INTO {table_name} VALUES \n"
    for index, record in df.iterrows():
        row_string = row_to_string(record)
        query += row_string
    query = ';'.join(query.rsplit(', \n', 1))
    return query

df = pd.read_csv(file_to_convert)

# data processing
# Strip symbols from the strings in the total column and convert to floats
# Convert the string values in date to datetimes
df['total'] = df['total'].replace("[$,]", "", regex=True).astype('float')
df['date'] = pd.to_datetime(df['date'])

# Connect to database
db_connection = sql.create_db_connection("localhost", "root", sql.get_password(), "sales")

# Insert Data
insertion_query = create_insertion_string(df)
sql.execute_query(db_connection, insertion_query)

# Disconnect from database?
db_connection.disconnect()