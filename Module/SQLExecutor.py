import oracledb
import psycopg
from psycopg.rows import dict_row
import configparser
import csv
from openpyxl import Workbook
from Module.Helpers import Logger

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
        self.logging = Logger.create_logger()
        self.logger = self.logging.getLogger("Postgres")
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
                row_factory=dict_row
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
        self.logging = Logger.create_logger()
        self.logger = self.logging.getLogger("Postgres")
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

    @property
    def description(self):
        """Expose the description attribute from the underlying cursor."""
        return self.__cursor.description

# Concrete class for Oracle database connection
class OracleConnection(GeneralConnection):
    def __init__(self):
        self.logging = Logger.create_logger()
        self.logger = self.logging.getLogger("Oracle")
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
        self.logging = Logger.create_logger()
        self.logger = self.logging.getLogger("Oracle")
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

    @property
    def description(self):
        """Expose the description attribute from the underlying cursor."""
        return self.__cursor.description
    
    def __apply_row_factory(self):
        """Applies the row factory to format rows as dictionaries with column names as keys."""
        columns = [col[0] for col in self.__cursor.description]
        self.__cursor.rowfactory = lambda *args: dict(zip(columns, args))

# SQLExecutor class manages the connection and execution of SQL queries
class SQLExecutor:
    def __init__(self, db_connection: GeneralConnection):
        self.logging = Logger.create_logger()
        self.logger = self.logging.getLogger("Executor")
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

    def execute_file(self, file_path, params=None):
        """Executes SQL queries from a file."""
        try:
            with open(file_path, 'r') as file:
                query = file.read()
            self.__cursor.execute(query, params)
            self.logger.info(f"Executed SQL from file: {file_path}")
        except FileNotFoundError as e:
            self.logger.error(f"SQL file not found: {file_path}")
            raise
        except Exception as e:
            self.logger.error(f"Failed to execute SQL file: {str(e)}")
            raise

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

    def save_to_csv(self, data, file_path):
        """Saves data to a CSV file with column names."""
        try:
            if not data:  # Check if data is empty
                self.logger.warning(f"No data to save to {file_path}. The file will not be created.")
                return
        
            if isinstance(data, dict):  # If data is a single dictionary, wrap it in a list
                data = [data]

            with open(file_path, 'w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=data[0].keys())
                writer.writeheader()
                writer.writerows(data)
            self.logger.info(f"Data saved to {file_path} as CSV.")
        except Exception as e:
            self.logger.error(f"Failed to save data to CSV: {str(e)}")
            raise

    def save_to_txt(self, data, file_path):
        """Saves data to a TXT file with column names, using tab-separated values."""
        try:
            if not data:  # Check if data is empty
                self.logger.warning(f"No data to save to {file_path}. The file will not be created.")
                return
        
            if isinstance(data, dict):  # If data is a single dictionary, wrap it in a list
                data = [data]

            with open(file_path, 'w') as file:
                # Writing headers
                file.write('\t'.join(data[0].keys()) + '\n')
                # Writing data
                for row in data:
                    file.write('\t'.join(map(str, row.values())) + '\n')
            self.logger.info(f"Data saved to {file_path} as TXT.")
        except Exception as e:
            self.logger.error(f"Failed to save data to TXT: {str(e)}")
            raise

    def save_to_excel(self, data, file_path):
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
            ws.append(list(data[0].keys()))
            # Writing data
            for row in data:
                ws.append(list(row.values()))
            wb.save(file_path)
            self.logger.info(f"Data saved to {file_path} as Excel.")
        except Exception as e:
            self.logger.error(f"Failed to save data to Excel: {str(e)}")
            raise