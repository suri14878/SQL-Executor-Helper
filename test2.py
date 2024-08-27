from Helper.SqlHelper import SqlHelper


# Example usage
executor = SqlHelper(environment='test',config_file='config.ini')
conn = executor.connect_oracle()
executor.run_query(conn, 'sample.sql', db_type='oracle', output_format='csv', output_file='output')
# executor.run_query('queries/sample.sql', db_type='postgres', output_format='csv', output_file='outputs/result')