# import cx_Oracle

# # Connection details
# dsn_tns = cx_Oracle.makedsn('localhost', 1521, service_name='orcl')

# # Connect to the database
# connection = cx_Oracle.connect(user='sys', password='Adarsh@0811', dsn=dsn_tns, mode=cx_Oracle.SYSDBA)

# # Create a cursor object
# cursor = connection.cursor()

# # Example: Executing a SQL query
# cursor.execute("SELECT * FROM actors")
# rows = cursor.fetchall()

# # Print the results
# for row in rows:
#     print(row)

# cursor.execute("SELECT COUNT(*) FROM actors")
# count = cursor.fetchone()
# print(f"Number of rows in Actors table: {count[0]}")

# # Close the cursor and connection
# cursor.close()
# connection.close()

import configparser

config = configparser.ConfigParser()
config.read('config.ini')

config['test_oracle']['host'],
config['test_oracle']['port'],
service_name=config['test_oracle']['service_name']

print(service_name)