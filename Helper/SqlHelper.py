import psycopg
import oracledb
import pandas as pd
import configparser

class SqlHelper:
    def __init__(self, environment='test', config_file='config.ini'):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)
        self.environment = environment


    def connect_postgres(self):
        env_section = f'{self.environment}_postgres'
        conn = psycopg2.connect(
            host=self.config[env_section]['host'],
            port=self.config[env_section]['port'],
            user=self.config[env_section]['user'],
            password=self.config[env_section]['password'],
            dbname=self.config[env_section]['dbname']
        )
        return conn

    def connect_oracle(self):
        env_section = f'{self.environment}_oracle'
        dsn = oracledb.makedsn(
            self.config[env_section]['host'],
            self.config[env_section]['port'],
            # service_name=self.config['test_oracle']['service_name']
            sid=self.config[env_section]['sid']
        )
        conn = oracledb.connect(
            user=self.config[env_section]['user'],
            password=self.config[env_section]['password'],
            dsn=dsn,
            # mode=oracledb.SYSDBA
        )

        return conn

    def run_query(self, conn, sql_file, db_type='postgres', output_format='csv', output_file='output'):
        with open(sql_file, 'r') as file:
            query = file.read()

        if db_type == 'postgres':
            with self.connect_postgres() as conn:
                df = pd.read_sql_query(query, conn)
        elif db_type == 'oracle':
            with self.connect_oracle() as conn:
                df = pd.read_sql_query(query, conn)

        if output_format == 'csv':
            df.to_csv(f'{output_file}.csv', index=False)
        elif output_format == 'txt':
            df.to_csv(f'{output_file}.txt', sep='\t', index=False)
        elif output_format == 'excel':
            df.to_excel(f'{output_file}.xlsx', index=False)
        else:
            raise ValueError("Unsupported format. Use 'csv', 'txt', or 'excel'.")

        return df
    
    def run_paginated_query(self, conn, sql_file, db_type='postgres', page_size=1000, output_format='csv', output_file='output'):
        with open(sql_file, 'r') as file:
            query = file.read()

        if db_type == 'postgres':
            with self.connect_postgres() as conn:
                offset = 0
                while True:
                    paginated_query = f"{query} LIMIT {page_size} OFFSET {offset}"
                    df = pd.read_sql_query(paginated_query, conn)
                    if df.empty:
                        break
                    df.to_csv(f'{output_file}_{offset}.csv', index=False)
                    offset += page_size

        elif db_type == 'oracle':
            with self.connect_oracle() as conn:
                offset = 0
                while True:
                    paginated_query = f"""
                    SELECT * FROM (
                        SELECT a.*, ROWNUM rnum FROM ({query}) a
                        WHERE ROWNUM <= {offset + page_size}
                    )
                    WHERE rnum > {offset}
                    """
                    df = pd.read_sql_query(paginated_query, conn)
                    if df.empty:
                        break
                    df.to_csv(f'{output_file}_{offset}.csv', index=False)
                    offset += page_size


# Example usage
# executor = SQLExecutor(environment='prod')
# executor.run_query('queries/sample.sql', db_type='postgres', output_format='csv', output_file='outputs/result')
