import unittest
import os,sys, shutil
import csv
from openpyxl import load_workbook

# Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from Module.SQLExecutor import SQLExecutor, OracleConnection, PostgresConnection
from Module.enums.file_types import FileType

class TestSQLExecutorIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Common Setup for initializing and testing databases."""

        # Can comment any database if want to test only the other.
        cls.databases = {
            'oracle': SQLExecutor(OracleConnection()),
            'postgres': SQLExecutor(PostgresConnection())
        }

        for db_type, db in cls.databases.items():
            db.connect(config_file='./Configs/Database_Config.ini', environment='test')
            if db_type == 'oracle':
                cls.setup_oracle(db)
            elif db_type == 'postgres':
                cls.setup_postgres(db)
        
        os.mkdir('./Test/TestFiles')
        os.mkdir('./Test/SQL Files')
        os.mkdir('./Test/TestScripts')

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

    @classmethod
    def setup_oracle(cls, db):
        """Setup Oracle-specific tables and data."""
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

    @classmethod
    def setup_postgres(cls, db):
        """Setup Postgres-specific tables and data."""
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


    def test_fetchAll_save_to_files(self):
        """Test Database features: save to CSV, TXT, Excel."""

        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                db.execute_file('./Test/SQL Files/singleQuery.sql')
                data = db.fetchall()
                self.save_and_verify_files(db, db_type, data, 'fetchAll')

    def test_fetchOne_save_to_files(self):
        """Test fetching one record from both databases and saving to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                db.execute_file('./Test/SQL Files/singleQuery.sql')
                data = [db.fetchone()]
                self.save_and_verify_files(db, db_type, data, 'fetchOne')
    
    def test_fetchMany_save_to_files(self):
        """Test fetching multiple records from both databases and saving to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                db.execute_file('./Test/SQL Files/singleQuery.sql')
                data = db.fetchmany(3)
                self.save_and_verify_files(db, db_type, data, 'fetchMany')
    
    def test_multiquery_to_file(self):
        """Test fetching multiple queries from single file from both databases and saving to CSV, TXT, Excel."""
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                db_list_data = db.execute_multiQuery_file('./Test/SQL Files/multipleQueries.sql')
                self.save_multiQuery_and_verify_files(db, db_type, db_list_data)

    def test_folder_to_file_and_verify_files(self):
        """Test fetching multiple files from folder from both databases and saving to CSV, TXT, Excel."""
        folder_path = './Test/TestScripts'
        save_path = './Test/TestFiles/'
        for db_type, db in self.databases.items():
            with self.subTest(db=db_type):
                # Check if the provided path is a valid directory
                if not os.path.isdir(folder_path):
                    return

                for filename in os.listdir(folder_path):
                    file_path = os.path.join(folder_path, filename)

                    # Check if the path is a file (and not a directory)
                    if os.path.isfile(file_path):
                        list_data = db.execute_multiQuery_file(file_path)
                        db.save_multiQuery_to_csv(list_data,f'{db_type}_{filename}',save_path)
                        db.save_multiQuery_to_txt(list_data,f'{db_type}_{filename}',save_path)
                        db.save_multiQuery_to_excel(list_data,f'{db_type}_{filename}',save_path)

                        for i, data in enumerate(list_data):
                            # Check if files exist
                            self.assertTrue(os.path.exists(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.csv')))
                            self.assertTrue(os.path.exists(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.txt')))
                            self.assertTrue(os.path.exists(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.xlsx')))

                            # Verify file contents
                            self.verify_csv_content(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.csv'), data)
                            self.verify_txt_content(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.txt'), data)
                            self.verify_excel_content(os.path.join(save_path, f'{db_type}_{filename}_{i+1}.xlsx'), data) 

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

    # Deleting all the testfiles and close connections
    @classmethod
    def tearDownClass(cls):
        """Tear down the test environment: drop the test tables and clean up."""
        for db_type, db in cls.databases.items():
            try:
                if db_type == 'oracle':
                    db.execute_query("DROP TABLE TestActors CASCADE CONSTRAINTS")
                elif db_type == 'postgres':
                    db.execute_query("DROP TABLE IF EXISTS TestActors CASCADE")
                db.execute_query("COMMIT")
            except Exception as e:
                print(f"Error dropping {db_type} table: {e}")
            finally:
                db.close()

        # Remove the TestFiles directory
        output_dir = './Test/TestFiles/'
        sqlFiles_dir= './Test/SQL Files'
        testScripts_dir= './Test/TestScripts'
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)
            shutil.rmtree(sqlFiles_dir)
            shutil.rmtree(testScripts_dir)
output_loc = './Test/TestFiles/'
if __name__ == '__main__':
    unittest.main()
