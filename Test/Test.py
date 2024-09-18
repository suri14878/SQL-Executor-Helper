import unittest
import os,sys, shutil
import csv
import configparser 
from openpyxl import load_workbook

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from Module.Helpers import Logger
from Module.SQLExecutor import SQLExecutor, OracleConnection, PostgresConnection
from Module.enums.file_types import FileType

class TestSQLExecutorIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Common Setup for initializing and testing databases."""

        # Load configuration settings
        config = cls.load_config()

        # Create Logger
        logging = Logger.create_logger()
        cls.logger = logging.getLogger('Tester')
        cls.logger.info("Setting up the databases and test environment...")

        # Can comment any database if want to test only the other.
        cls.databases = {}
        
        if config.getboolean('TestSettings', 'TestOracle'):
            cls.databases['oracle'] = SQLExecutor(OracleConnection())
        
        if config.getboolean('TestSettings', 'TestPostgres'):
            cls.databases['postgres'] = SQLExecutor(PostgresConnection())

        # Connect to the databases
        for db_type, db in cls.databases.items():
            try:
                db.connect(config_file='./Configs/Database_Config.ini', environment='test')
                cls.logger.info(f"Connected to {db_type} database successfully.")
                if db_type == 'oracle':
                    cls.setup_oracle(db)
                elif db_type == 'postgres':
                    cls.setup_postgres(db)
            except Exception as e:
                cls.logger.error(f"Failed to connect to {db_type} database: {e}")
        
        os.mkdir('./Test/TestFiles')
        os.mkdir('./Test/SQL Files')
        os.mkdir('./Test/TestScripts')

        # Create necessary SQL files for testing
        cls.logger.info("Creating test SQL files...")

        try:
            with open('./Test/SQL Files/singleQuery.sql', 'w') as file:
                    file.write('SELECT * FROM TestActors')
            with open('./Test/SQL Files/multipleQueries.sql', 'w') as file:
                file.write('''SELECT * FROM TestActors;
                        SELECT * FROM TestActors
                        FETCH FIRST 3 ROWS ONLY;
                        SELECT * FROM TestActors
                        FETCH FIRST 2 ROWS ONLY;
                            ''')
            with open('./Test/TestScripts/Adarsh.sql', 'w') as file:
                file.write('''SELECT * FROM TestActors;
                        SELECT * FROM TestActors
                        FETCH FIRST 3 ROWS ONLY;
                            ''')
            with open('./Test/TestScripts/Blake.sql', 'w') as file:
                file.write('''SELECT * FROM TestActors
                        FETCH FIRST 2 ROWS ONLY;
                        SELECT * FROM TestActors;
                            ''')
            cls.logger.info("Test SQL files created successfully.")
        except Exception as e:
            cls.logger.error(f"Error creating test SQL files: {e}")

    @classmethod
    def setup_oracle(cls, db):
        """Setup Oracle-specific tables and data."""
        cls.logger.info("Setting up Oracle database...")
        try:
            db.execute_query("""
            BEGIN
               EXECUTE IMMEDIATE 'DROP TABLE TestActors CASCADE CONSTRAINTS';
            EXCEPTION
               WHEN OTHERS THEN
                  IF SQLCODE != -942 THEN
                     RAISE;
                  END IF;
            END;
            """)
            db.execute_query("""
                CREATE TABLE TestActors (
                    PK_ID INTEGER PRIMARY KEY,
                    NAME VARCHAR(100),
                    SEX VARCHAR(10),
                    BIO VARCHAR(1000)
                )
            """)
            for i in range(1, 6):
                db.execute_query(f"""
                    INSERT INTO TestActors (PK_ID, NAME, SEX, BIO)
                    VALUES ({i}, 'Actor {i}', 'Male', 'Bio of Actor {i}')
                """)
            db.execute_query("COMMIT")
            cls.logger.info("Oracle database setup completed.")
        except Exception as e:
            cls.logger.error(f"Error setting up Oracle database: {e}")

    @classmethod
    def setup_postgres(cls, db):
        """Setup Postgres-specific tables and data."""
        cls.logger.info("Setting up Postgres database...")
        try:
            db.execute_query("DROP TABLE IF EXISTS TestActors CASCADE;")
            db.execute_query("""
                CREATE TABLE TestActors (
                    PK_ID SERIAL PRIMARY KEY,
                    NAME VARCHAR(100),
                    SEX VARCHAR(10),
                    BIO TEXT
                )
            """)
            for i in range(1, 6):
                db.execute_query(f"""
                    INSERT INTO TestActors (NAME, SEX, BIO)
                    VALUES ('Actor {i}', 'Male', 'Bio of Actor {i}')
                """)
            db.execute_query("COMMIT")
            cls.logger.info("Postgres database setup completed.")
        except Exception as e:
            cls.logger.error(f"Error setting up Postgres database: {e}")


    def test_fetchAll_save_to_files(self):
        """Test Database features: save to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                self.logger.info(f"Running fetchAll test for {db_type} database.")
                try:
                    db.execute_file('./Test/SQL Files/singleQuery.sql')
                    data = db.fetchall()
                    self.save_and_verify_files(db, db_type, data, 'fetchAll')
                except Exception as e:
                    self.logger.error(f"Error in fetchAll test for {db_type}: {e}")

    def test_fetchOne_save_to_files(self):
        """Test fetching one record from both databases and saving to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                self.logger.info(f"Running fetchOne test for {db_type} database.")
                try:
                    db.execute_file('./Test/SQL Files/singleQuery.sql')
                    data = [db.fetchone()]
                    self.save_and_verify_files(db, db_type, data, 'fetchOne')
                except Exception as e:
                    self.logger.error(f"Error in fetchOne test for {db_type}: {e}")

    def test_fetchMany_save_to_files(self):
        """Test fetching multiple records from both databases and saving to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                self.logger.info(f"Running fetchMany test for {db_type} database.")
                try:
                    db.execute_file('./Test/SQL Files/singleQuery.sql')
                    data = db.fetchmany(3)
                    self.save_and_verify_files(db, db_type, data, 'fetchMany')
                except Exception as e:
                    self.logger.error(f"Error in fetchMany test for {db_type}: {e}")

    def test_multiquery_to_file(self):
        """Test fetching multiple queries from single file from both databases and saving to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                self.logger.info(f"Running multi-query test for {db_type} database.")
                try:
                    db_list_data = db.execute_multiQuery_file('./Test/SQL Files/multipleQueries.sql')
                    self.save_multiQuery_and_verify_files(db, db_type, db_list_data)
                except Exception as e:
                    self.logger.error(f"Error in multi-query test for {db_type}: {e}")

    def test_folder_to_file_and_verify_files(self):
        """Test fetching multiple files from folder from both databases and saving to CSV, TXT, Excel."""
        folder_path = './Test/TestScripts'
        save_path = './Test/TestFiles/'
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                self.logger.info(f"Running folder query test for {db_type} database.")
                try:
                    # Check if the provided path is a valid directory
                    if not os.path.isdir(folder_path):
                        self.logger.warning(f"Directory {folder_path} does not exist.")
                        return

                    for filename in os.listdir(folder_path):
                        file_path = os.path.join(folder_path, filename)

                        # Check if the path is a file (and not a directory)
                        if os.path.isfile(file_path):
                            list_data = db.execute_multiQuery_file(file_path)
                            db.save_multiQuery_to_csv(list_data, f'{db_type}_{filename}', save_path)
                            db.save_multiQuery_to_txt(list_data, f'{db_type}_{filename}', save_path)
                            db.save_multiQuery_to_excel(list_data, f'{db_type}_{filename}', save_path)

                            for i, data in enumerate(list_data):
                                # Check if files exist
                                self.assertTrue(os.path.exists(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.csv')))
                                self.assertTrue(os.path.exists(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.txt')))
                                self.assertTrue(os.path.exists(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.xlsx')))

                                # Verify file contents
                                self.verify_csv_content(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.csv'), data)
                                self.verify_txt_content(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.txt'), data)
                                self.verify_excel_content(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.xlsx'), data)
                except Exception as e:
                    self.logger.error(f"Error in folder query test for {db_type}: {e}")

    def save_multiQuery_and_verify_files(self, db, db_type, list_data):
        """Helper method to save multiquery data and verify CSV, TXT, Excel files."""
        output_loc = './Test/TestFiles/'
        db.save_multiQuery_to_csv(list_data, f'{db_type}_test', output_loc)
        db.save_multiQuery_to_txt(list_data, f'{db_type}_test', output_loc)
        db.save_multiQuery_to_excel(list_data, f'{db_type}_test', output_loc)

        output_dir = './Test/TestFiles'
        
        for i, data in enumerate(list_data):
            # Check if files exist
            self.assertTrue(os.path.exists(os.path.join(output_dir, f'{db_type}_test_{i+1}.csv')))
            self.assertTrue(os.path.exists(os.path.join(output_dir, f'{db_type}_test_{i+1}.txt')))
            self.assertTrue(os.path.exists(os.path.join(output_dir, f'{db_type}_test_{i+1}.xlsx')))

            # Verify file contents
            self.verify_csv_content(os.path.join(output_dir, f'{db_type}_test_{i+1}.csv'), data)
            self.verify_txt_content(os.path.join(output_dir, f'{db_type}_test_{i+1}.txt'), data)
            self.verify_excel_content(os.path.join(output_dir, f'{db_type}_test_{i+1}.xlsx'), data)


    def save_and_verify_files(self, db, db_type, data, fetch_type):
        """Helper method to save data and verify CSV, TXT, Excel files."""
        output_dir = './Test/TestFiles'
        db.save_to_csv(data, os.path.join(output_dir, f'{db_type}_{fetch_type}_test.csv'))
        db.save_to_txt(data, os.path.join(output_dir, f'{db_type}_{fetch_type}_test.txt'))
        db.save_to_excel(data, os.path.join(output_dir, f'{db_type}_{fetch_type}_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, f'{db_type}_{fetch_type}_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, f'{db_type}_{fetch_type}_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, f'{db_type}_{fetch_type}_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, f'{db_type}_{fetch_type}_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, f'{db_type}_{fetch_type}_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, f'{db_type}_{fetch_type}_test.xlsx'), data)

    # Verification methods...
    def verify_csv_content(self, file_path, expected_data):
        """Verify the content of the CSV file."""
        with open(file_path, mode='r') as file:
            reader = csv.DictReader(file)
            rows = list(reader)
            self.assertEqual(len(rows), len(expected_data))
            for i, row in enumerate(rows):
                for key, value in row.items():
                    self.assertEqual(str(value), str(expected_data[i][key]) if  str(expected_data[i][key]) != 'None' else '')

    def verify_txt_content(self, file_path, expected_data):
        """Verify the content of the TXT file."""
        with open(file_path, mode='r') as file:
            lines = file.readlines()
            self.assertEqual(len(lines) - 1, len(expected_data))  # -1 for the header
            headers = lines[0].strip().split('\t')
            for i, line in enumerate(lines[1:]):
                values = line.strip().split('\t')
                row = dict(zip(headers, values))
                for key in headers:
                    self.assertEqual(str(row[key]), str(expected_data[i][key]))

    def verify_excel_content(self, file_path, expected_data):
        """Verify the content of the Excel file."""
        workbook = load_workbook(filename=file_path)
        sheet = workbook.active
        headers = [cell.value for cell in sheet[1]]
        for i, row in enumerate(sheet.iter_rows(values_only=True, min_row=2)):
            row_data = dict(zip(headers, row))
            for key in headers:
                self.assertEqual(str(row_data[key]), str(expected_data[i][key]) if  str(expected_data[i][key]) != 'None' else '')

    @classmethod
    def load_config(cls):
        """Load the configuration settings from TestConfig.ini."""
        config = configparser.ConfigParser()
        config.read('./Configs/Test_Config.ini')
        return config
    
    # Deleting all the testfiles and close connections
    @classmethod
    def tearDownClass(cls):
        """Tear down the test environment: drop the test tables and clean up."""

        config = cls.load_config()

        cls.logger.info("Tearing down test environment...")
        for db_type, db in cls.databases.items():
            try:
                if db_type == 'oracle':
                    db.execute_query("DROP TABLE TestActors CASCADE CONSTRAINTS")
                elif db_type == 'postgres':
                    db.execute_query("DROP TABLE IF EXISTS TestActors CASCADE")
                db.execute_query("COMMIT")
                cls.logger.info(f"{db_type} test table dropped.")
            except Exception as e:
                cls.logger.error(f"Error dropping {db_type} table: {e}")
            finally:
                db.close()
                cls.logger.info(f"{db_type} connection closed.")

        # Decide whether to clean up files based on config
        if config.getboolean('TestSettings', 'CleanUpFiles'):
            output_dir = './Test/TestFiles/'
            sqlFiles_dir = './Test/SQL Files'
            testScripts_dir = './Test/TestScripts'
            try:
                if os.path.exists(output_dir):
                    shutil.rmtree(output_dir)
                if os.path.exists(sqlFiles_dir):
                    shutil.rmtree(sqlFiles_dir)
                if os.path.exists(testScripts_dir):
                    shutil.rmtree(testScripts_dir)
                cls.logger.info("Test directories and files cleaned up.")
            except Exception as e:
                cls.logger.error(f"Error during test cleanup: {e}")

if __name__ == '__main__':
    unittest.main()
