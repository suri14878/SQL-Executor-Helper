from Module.SQLExecutor import SQLExecutor,OracleConnection,PostgresConnection

# Testing Oracle Database

oracle_db = SQLExecutor(OracleConnection())
oracle_db.connect(config_file='./Configs/Database_Config.ini',environment='test')
oracle_db.execute_file(file_path='./SQL Files/sample.sql')
oracle_rows = oracle_db.fetchall()

for row in oracle_rows:
    print(row)

# Testing Postgres Database

# postgres_db = SQLExecutor(PostgresConnection())
# postgres_db.connect(config_file='./Configs/Database_Config.ini',environment='test')
# postgres_db.execute_file(file_path='./SQL Files/sample.sql')
# postgres_rows = postgres_db.fetchall()

# for row in postgres_rows:
#     print(row)
