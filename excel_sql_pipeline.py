import pandas as pd
import sql 

df = pd.read_csv("MOCK_DATA.csv")

# print(df.head())
# print(df.info())

df['total'] = df['total'].replace("[$,]", "", regex=True).astype('float')

print(df.head())
print(df.info())

# Any other data processing

# Connect to database
db_connection = sql.create_db_connection("localhost", "root", sql.get_password(), "sales")

# Insert Data

# Disconnect from database?
db_connection.disconnect()