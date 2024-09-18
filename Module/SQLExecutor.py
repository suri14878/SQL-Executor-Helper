import os
import oracledb
import psycopg
import configparser
import csv
import re
from openpyxl import Workbook
from Module.Helpers import Logger
from Module.enums.file_types import FileType


logging = Logger.create_logger()

class UniqueDictRowFactory:
    def __init__(self, cursor: psycopg.Cursor):
        # Check if the query returns a result set (i.e., cursor.description is not None)
        if cursor.description is None:
            self.fields = None  # No fields to process if there's no result set
        else:
            # Extract column names from cursor.description
            self.fields = []
            field_count = {}
            for col in cursor.description:
                col_name = col.name
                if col_name in field_count:
                    # If the column name already exists, append a unique identifier
                    field_count[col_name] += 1
                    col_name = f"{col_name}_{field_count[col_name]}"
                else:
                    field_count[col_name] = 0
                self.fields.append(col_name)

    def __call__(self, values):
        # If there's no result set (fields are None), return None or an empty dict
        if self.fields is None:
            return {}
        # Otherwise, create a dictionary mapping the unique column names to values
        return dict(zip(self.fields, values))


# Abstract base class defining the general interface for all database connections
class GeneralConnection:
    def __init__(self):
        raise NotImplementedError("Subclass must implement this method")
    
    def connect(self, config_file, environment):
        raise NotImplementedError("Subclass must implement this method")

    def close(self):
        raise NotImplementedError("Subclass must implement this method")

    def get_cursor(self):
        raise NotImplementedError("Subclass must implement this method")

# Abstract base class defining the general interface for all database cursors
class GeneralCursor:
    def execute(self, query, params=None):
        raise NotImplementedError("Subclass must implement this method")
    
    def fetchall(self):
        raise NotImplementedError("Subclass must implement this method")
    
    def fetchmany(self, size):
        raise NotImplementedError("Subclass must implement this method")
    
    def fetchone(self):
        raise NotImplementedError("Subclass must implement this method")
    
    @property
    def description(self):
        """Expose the description attribute from the underlying cursor."""
        raise NotImplementedError("Subclass must implement this method")

# Concrete class for PostgreSQL database connection
class PostgresConnection(GeneralConnection):
    def __init__(self):
        self.logger = logging.getLogger("Postgres")
        self.__connection = None

    def connect(self, config_file, environment):
        """Establishes a connection to a PostgreSQL database using the provided config file and environment."""
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
            env_section = f'{environment}_postgres'
            self.__connection = psycopg.connect(
                host=config[env_section]['host'],
                port=config[env_section]['port'],
                user=config[env_section]['user'],
                password=config[env_section]['password'],
                dbname=config[env_section]['dbname'],
                row_factory=UniqueDictRowFactory
            )
            self.logger.info(f"Connected to PostgreSQL database: {config[env_section]['dbname']}")

        except (psycopg.OperationalError, configparser.Error) as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {str(e)}")
            raise

    def close(self):
        """Closes the PostgreSQL database connection."""
        if self.__connection:
            self.__connection.close()
            self.logger.info("PostgreSQL connection closed.")

    def get_cursor(self):
        """Returns a generalized cursor object for PostgreSQL."""
        try:
            return PostgresCursor(self.__connection.cursor())
        except Exception as e:
            self.logger.error(f"Failed to create PostgreSQL cursor: {str(e)}")
            raise

# Concrete class for PostgreSQL cursor
class PostgresCursor(GeneralCursor):
    def __init__(self, cursor):
        self.logger = logging.getLogger("Postgres")
        self.__cursor = cursor

    def execute(self, query, params=None):
        """Executes a SQL query using the PostgreSQL cursor."""
        try:
            self.__cursor.execute(query, params)
            self.logger.info("Executed query on PostgreSQL.")
        except psycopg.Error as e:
            self.logger.error(f"Failed to execute query on PostgreSQL: {str(e)}")
            raise

    def fetchone(self):
        """Fetches one row from the query result."""
        return self.__cursor.fetchone()

    def fetchall(self):
        """Fetches all rows from the query result."""
        return self.__cursor.fetchall()

    def fetchmany(self, size):
        """Fetches a specific number of rows from the query result."""
        return self.__cursor.fetchmany(size)
    
    def fetch_paginated(self, query, page_size, page_number, params=None):
        """Fetches paginated results from the query result for PostgreSQL."""
        paginated_query = f"{query} LIMIT {page_size} OFFSET {page_size * (page_number - 1)}"
        self.execute(paginated_query, params)
        return self.fetchall()

    @property
    def description(self):
        """Expose the description attribute from the underlying cursor."""
        return self.__cursor.description

# Concrete class for Oracle database connection
class OracleConnection(GeneralConnection):
    def __init__(self):
        self.logger = logging.getLogger("Oracle")
        self.__connection = None

    def connect(self, config_file, environment):
        """Establishes a connection to an Oracle database using the provided config file and environment."""
        try:
            config = configparser.ConfigParser()
            config.read(config_file)
            env_section = f'{environment}_oracle'
            dsn = oracledb.makedsn(
                config[env_section]['host'],
                config[env_section]['port'],
                sid=config[env_section]['sid']
            )
            self.__connection = oracledb.connect(
                user=config[env_section]['user'],
                password=config[env_section]['password'],
                dsn=dsn
            )
            self.logger.info(f"Connected to Oracle database: {config[env_section]['sid']}")

        except (oracledb.DatabaseError, configparser.Error) as e:
            self.logger.error(f"Failed to connect to Oracle: {str(e)}")
            raise

    def close(self):
        """Closes the Oracle database connection."""
        if self.__connection:
            self.__connection.close()
            self.logger.info("Oracle connection closed.")

    def get_cursor(self):
        """Returns a generalized cursor object for Oracle."""
        try:
            return OracleCursor(self.__connection.cursor())
        except Exception as e:
            self.logger.error(f"Failed to create Oracle cursor: {str(e)}")
            raise

# Concrete class for Oracle cursor
class OracleCursor(GeneralCursor):
    def __init__(self, cursor):
        self.logger = logging.getLogger("Oracle")
        self.__cursor = cursor

    def execute(self, query, params=None):
        """Executes a SQL query using the Oracle cursor."""
        try:
            self.__cursor.execute(query, params or {})
            self.logger.info("Executed query on Oracle.")

        except oracledb.DatabaseError as e:
            self.logger.error(f"Failed to execute query on Oracle: {str(e)}")
            raise

    def fetchone(self):
        """Fetches one row from the query result."""
        self.__apply_row_factory()
        return self.__cursor.fetchone()

    def fetchall(self):
        """Fetches all rows from the query result."""
        self.__apply_row_factory()
        return self.__cursor.fetchall()

    def fetchmany(self, size):
        """Fetches a specific number of rows from the query result."""
        self.__apply_row_factory()
        return self.__cursor.fetchmany(size)
    
    def fetch_paginated(self, query, page_size, page_number, params=None):
        """Fetches paginated results from the query for Oracle."""
        offset_value = (page_number - 1) * page_size
        paginated_query = f"""
            {query}
            OFFSET {offset_value} ROWS 
            FETCH NEXT {page_size} ROWS ONLY
        """
        self.execute(paginated_query, params)
        return self.fetchall()

    @property
    def description(self):
        """Expose the description attribute from the underlying cursor."""
        return self.__cursor.description
    
    def __apply_row_factory(self):
        """Applies the row factory to format rows as dictionaries with column names as keys, handling duplicate column names."""
        # Extract column names from cursor description
        columns = [col[0] for col in self.__cursor.description]

        # Create a dictionary to count occurrences of column names
        field_count = {}
        unique_columns = []

        # Handle duplicate column names by appending a suffix
        for col_name in columns:
            if col_name in field_count:
                # If the column name already exists, append a unique identifier
                field_count[col_name] += 1
                unique_col_name = f"{col_name}_{field_count[col_name]}"
            else:
                field_count[col_name] = 0
                unique_col_name = col_name
            unique_columns.append(unique_col_name)

        # Set rowfactory to map unique column names to values
        self.__cursor.rowfactory = lambda *args: dict(zip(unique_columns, args))


# SQLExecutor class manages the connection and execution of SQL queries
class SQLExecutor:
    def __init__(self, db_connection: GeneralConnection):
        self.logger = logging.getLogger("Executor")
        self.__db_connection = db_connection
        self.__cursor = None

    def connect(self, config_file='config.ini', environment='test'):
        """Connects to the database using the provided configuration file and environment."""
        self.__db_connection.connect(config_file, environment)
        self.__cursor = self.__db_connection.get_cursor()

    def close(self):
        """Closes the database connection."""
        self.__db_connection.close()

    def execute_query(self, query, params=None):
        """Executes a SQL query on the connected database."""
        self.__cursor.execute(query, params)

    def execute_file(self, file_path, make_batches=False, make_paginated=False, page_size=0, page_number=1, params=None):
        """Executes SQL queries from a file."""
        try:
            with open(file_path, 'r') as file:
                query = file.read()

            if make_batches and make_paginated:
                return self.get_batches_by_query(query,page_size,params)
            elif make_batches:
                self.__cursor.fetch_paginated(query, page_size, page_number, params)
            else:
                self.__cursor.execute(query, params)
                self.logger.info(f"Executed SQL from file: {file_path}")
        except FileNotFoundError as e:
            self.logger.error(f"SQL file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to execute SQL file: {str(e)}")
            raise

    def extract_pagination_info(self, query):
        """Extracts the pagination information from multiline comments using the pattern '/* PAGINATE SIZE <number> */'."""
        match = re.search(r'/\*\s*PAGINATE\s+SIZE\s+(\d+)\s*\*/', query, flags=re.IGNORECASE)
        if match:
            return True, int(match.group(1))  # Return pagination flag and page size as an integer
        return False, None  # Return False if no pagination information is found


    def get_batches_by_query(self,query, page_size, params=None):
        _page_number = 1
        while True:
            results = self.__cursor.fetch_paginated(query, page_size, _page_number, params)
            if not results:
                break  # No more results, stop pagination
            yield results  # Yield the current page's results
            _page_number += 1  # Move to the next page

    def execute_multiQuery_file(self, file_path, params=None):
        """Executes multiple SQL queries from a file."""
        try:
            with open(file_path, 'r') as file:
                queries = file.read()

            all_results = []  # Store results from all queries

            # Split the queries by semicolon, and execute each query
            for query in queries.split(';'):
                query = query.strip()
                if not query:  # Skip empty statements
                    continue

                # Check for multiline comment '/* PAGINATE SIZE <number> */'
                paginate, page_size = self.extract_pagination_info(query)

                # Remove the multiline comments from the query
                query = re.sub(r'/\*.*?\*/', '', query, flags=re.DOTALL)

                if paginate and page_size is not None:
                    # Paginate this query with the specified page size
                    batches = self.get_batches_by_query(query, page_size, params)
                    all_batches = []
                    for batch in batches:
                        all_batches.extend(batch)

                    all_results.append(all_batches)
                else:
                    # Execute the query normally
                    self.execute_query(query, params)

                    # If it's a SELECT query, fetch the results
                    if query.lower().startswith('select'):
                        results = self.fetchall()
                        all_results.append(results)

            return all_results
        
        except FileNotFoundError as e:
            self.logger.error(f"SQL file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to execute SQL file: {str(e)}")
            raise

    def execute_folder_and_save(self, folder_path, file_type: FileType, save_path=''):
        """Executes multiple SQL files from a folder."""
        
        # Check if the provided path is a valid directory
        if not os.path.isdir(folder_path):
            self.logger.error(f"The provided path '{folder_path}' is not a valid directory.")
            return

        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)

            # Check if the path is a file (and not a directory)
            if os.path.isfile(file_path):
                self.logger.info(f"Reading file: {filename}")
                data = self.execute_multiQuery_file(file_path)
                if file_type == FileType.CSV:
                    self.save_multiQuery_to_csv(data,filename,save_path)
                elif file_type == FileType.TXT:
                    self.save_multiQuery_to_txt(data,filename,save_path)
                elif file_type == FileType.EXCEL:
                    self.save_multiQuery_to_excel(data,filename,save_path)
                

    def fetchone(self):
        """Fetches one row from the last executed query."""
        try:
            return self.__cursor.fetchone()
        except Exception as e:
            self.logger.error(f"Failed to fetch one row: {str(e)}")
            raise

    def fetchall(self):
        """Fetches all rows from the last executed query."""
        try:
            return self.__cursor.fetchall()
        except Exception as e:
            self.logger.error(f"Failed to fetch all rows: {str(e)}")
            raise

    def fetchmany(self, size):
        """Fetches a specific number of rows from the last executed query."""
        try:
            return self.__cursor.fetchmany(size)
        except Exception as e:
            self.logger.error(f"Failed to fetch {size} rows: {str(e)}")
            raise
        
    def save_batches(self, batches, result_file, result_file_type: FileType):
        """Appends these batches in specified file"""
        for i, batch in enumerate(batches):
            if result_file_type == FileType.CSV:
                self.save_to_csv(batch,result_file,is_append=True,include_header=True if i==0 else False)    
            elif result_file_type == FileType.TXT:
                self.save_to_txt(batch,result_file,is_append=True,include_header=True if i==0 else False)    
            elif result_file_type == FileType.EXCEL:
                self.save_to_excel(batch,result_file, include_header=True if i==0 else False)    

    def save_multiQuery_to_csv(self, list_data, file_name, file_path=''):
        """Saves data to a CSV file with column names."""
        self.template_for_saving_data(list_data, file_name, file_path, self.save_to_csv, 'csv')

    def save_multiQuery_to_txt(self, list_data, file_name, file_path=''):
        """Saves data to a Text file with column names."""
        self.template_for_saving_data(list_data, file_name, file_path, self.save_to_txt, 'txt')

    def save_multiQuery_to_excel(self, list_data, file_name, file_path=''):
        """Saves data to a Excel file with column names."""
        self.template_for_saving_data(list_data, file_name, file_path, self.save_to_excel, 'xlsx')

    def template_for_saving_data(self, list_data, file_name, file_path, save_function, file_type):
        try:
            if not list_data:  # Check if data is empty
                self.logger.warning(f"No data to save to {file_name}. The file will not be created.")
                return
            
            preceding_zeros = len(str(len(list_data)))

            for i,data in enumerate(list_data):
                save_function(data,f"{file_path}{file_name}_{str(i+1).zfill(preceding_zeros)}.{file_type}")
        except Exception as e:
            self.logger.error(f"Failed to save data to {file_type}: {str(e)}")
            raise


    def save_to_csv(self, data, file_path, is_append=False, include_header=True):
        """Saves data to a CSV file with column names."""
        try:
            if not data:  # Check if data is empty
                self.logger.warning(f"No data to save to {file_path}. The file will not be created.")
                return
        
            if isinstance(data, dict):  # If data is a single dictionary, wrap it in a list
                data = [data]

            with open(file_path, 'a' if is_append else 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                if include_header:
                    writer.writeheader()
                writer.writerows(data)
            self.logger.info(f"Data saved to {file_path} as CSV.")
        except Exception as e:
            self.logger.error(f"Failed to save data to CSV: {str(e)}")
            raise

    def save_to_txt(self, data, file_path, is_append=False, include_header=True):
        """Saves data to a TXT file with column names, using tab-separated values."""
        try:
            if not data:  # Check if data is empty
                self.logger.warning(f"No data to save to {file_path}. The file will not be created.")
                return
        
            if isinstance(data, dict):  # If data is a single dictionary, wrap it in a list
                data = [data]

            with open(file_path, 'a' if is_append else 'w', newline='') as file:
                # Writing headers
                if include_header:
                    file.write('\t'.join(data[0].keys()) + '\n')
                # Writing data
                for row in data:
                    file.write('\t'.join(map(str, row.values())) + '\n')
            self.logger.info(f"Data saved to {file_path} as TXT.")
        except Exception as e:
            self.logger.error(f"Failed to save data to TXT: {str(e)}")
            raise

    def save_to_excel(self, data, file_path, include_header=True):
        """Saves data to an Excel file with column names."""
        try:
            if not data:  # Check if data is empty
                self.logger.warning(f"No data to save to {file_path}. The file will not be created.")
                return
        
            if isinstance(data, dict):  # If data is a single dictionary, wrap it in a list
                data = [data]

            wb = Workbook()
            ws = wb.active
            # Writing headers
            if include_header:
                ws.append(list(data[0].keys()))
            # Writing data
            for row in data:
                ws.append(list(row.values()))
            wb.save(file_path)
            self.logger.info(f"Data saved to {file_path} as Excel.")
        except Exception as e:
            self.logger.error(f"Failed to save data to Excel: {str(e)}")
            raise