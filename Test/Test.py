import unittest
import os,sys, shutil
import csv
from openpyxl import load_workbook

# # Get the parent directory
parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
from Module.SQLExecutor import SQLExecutor, OracleConnection, PostgresConnection

class TestSQLExecutorIntegration(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        """Setup the test environment: create tables and insert test data."""
        cls.oracle_db = SQLExecutor(OracleConnection())
        cls.oracle_db.connect(config_file='./Configs/Database_Config.ini', environment='test')
        
        # Drop and create the TestActors table in Oracle
        cls.oracle_db.execute_query("""
        BEGIN
           EXECUTE IMMEDIATE 'DROP TABLE TestActors CASCADE CONSTRAINTS';
        EXCEPTION
           WHEN OTHERS THEN
              IF SQLCODE != -942 THEN
                 RAISE;
              END IF;
        END;
        """)
                
        cls.oracle_db.execute_query("""
            CREATE TABLE TestActors (
                PK_ID INTEGER PRIMARY KEY,
                NAME VARCHAR(100),
                SEX VARCHAR(10),
                BIO VARCHAR(1000)
            )
        """)
        for i in range(1, 6):
            cls.oracle_db.execute_query(f"""
                INSERT INTO TestActors (PK_ID, NAME, SEX, BIO)
                VALUES ({i}, 'Actor {i}', 'Male', 'Bio of Actor {i}')
            """)
        cls.oracle_db.execute_query("COMMIT")
        cls.postgres_db = SQLExecutor(PostgresConnection())
        cls.postgres_db.connect(config_file='./Configs/Database_Config.ini', environment='test')
        
        # Drop and create the TestActors table in Postgres
        cls.postgres_db.execute_query("""
        DROP TABLE IF EXISTS TestActors CASCADE;
        """)
        
        cls.postgres_db.execute_query("""
            CREATE TABLE TestActors (
                PK_ID SERIAL PRIMARY KEY,
                NAME VARCHAR(100),
                SEX VARCHAR(10),
                BIO TEXT
            )
        """)
        
        for i in range(1, 6):
            cls.postgres_db.execute_query(f"""
                INSERT INTO TestActors (NAME, SEX, BIO)
                VALUES ('Actor {i}', 'Male', 'Bio of Actor {i}')
            """)
        cls.postgres_db.execute_query("COMMIT")

        os.mkdir('./Test/TestFiles') 


    def test_oracle_fetchAll_save_to_files(self):
        """Test Oracle DB features: save to CSV, TXT, Excel."""
        self.oracle_db.execute_file('./Test/SQL Files/sample.sql')
        data = self.oracle_db.fetchall()

        # Save to files
        output_dir = './Test/TestFiles'
        self.oracle_db.save_to_csv(data, os.path.join(output_dir, 'oracle_fetchAll_test.csv'))
        self.oracle_db.save_to_txt(data, os.path.join(output_dir, 'oracle_fetchAll_test.txt'))
        self.oracle_db.save_to_excel(data, os.path.join(output_dir, 'oracle_fetchAll_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchAll_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchAll_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchAll_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, 'oracle_fetchAll_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, 'oracle_fetchAll_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, 'oracle_fetchAll_test.xlsx'), data)

    def test_postgres_fetchAll_save_to_files(self):
        """Test Postgres DB features: save to CSV, TXT, Excel."""
        self.postgres_db.execute_file('./Test/SQL Files/sample.sql')
        data = self.postgres_db.fetchall()

        # Save to files
        output_dir = './Test/TestFiles'
        self.postgres_db.save_to_csv(data, os.path.join(output_dir, 'postgres_fetchAll_test.csv'))
        self.postgres_db.save_to_txt(data, os.path.join(output_dir, 'postgres_fetchAll_test.txt'))
        self.postgres_db.save_to_excel(data, os.path.join(output_dir, 'postgres_fetchAll_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchAll_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchAll_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchAll_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, 'postgres_fetchAll_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, 'postgres_fetchAll_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, 'postgres_fetchAll_test.xlsx'), data)

    def test_oracle_fetchOne_save_to_files(self):
        """Test Oracle DB features: save to CSV, TXT, Excel."""
        self.oracle_db.execute_file('./Test/SQL Files/sample.sql')
        data = self.oracle_db.fetchone()

        # Wrap single row in a list
        data = [data]

        # Save to files
        output_dir = './Test/TestFiles'
        self.oracle_db.save_to_csv(data, os.path.join(output_dir, 'oracle_fetchOne_test.csv'))
        self.oracle_db.save_to_txt(data, os.path.join(output_dir, 'oracle_fetchOne_test.txt'))
        self.oracle_db.save_to_excel(data, os.path.join(output_dir, 'oracle_fetchOne_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchOne_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchOne_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchOne_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, 'oracle_fetchOne_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, 'oracle_fetchOne_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, 'oracle_fetchOne_test.xlsx'), data)

    def test_postgres_fetchOne_save_to_files(self):
        """Test Postgres DB features: save to CSV, TXT, Excel."""
        self.postgres_db.execute_file('./Test/SQL Files/sample.sql')
        data = self.postgres_db.fetchone()

        # Wrap single row in a list
        data = [data]

        # Save to files
        output_dir = './Test/TestFiles'
        self.postgres_db.save_to_csv(data, os.path.join(output_dir, 'postgres_fetchOne_test.csv'))
        self.postgres_db.save_to_txt(data, os.path.join(output_dir, 'postgres_fetchOne_test.txt'))
        self.postgres_db.save_to_excel(data, os.path.join(output_dir, 'postgres_fetchOne_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchOne_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchOne_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchOne_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, 'postgres_fetchOne_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, 'postgres_fetchOne_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, 'postgres_fetchOne_test.xlsx'), data)

    def test_oracle_fetchMany_save_to_files(self):
        """Test Oracle DB features: save to CSV, TXT, Excel."""
        self.oracle_db.execute_file('./Test/SQL Files/sample.sql')
        data = self.oracle_db.fetchmany(3)

        # Save to files
        output_dir = './Test/TestFiles'
        self.oracle_db.save_to_csv(data, os.path.join(output_dir, 'oracle_fetchMany_test.csv'))
        self.oracle_db.save_to_txt(data, os.path.join(output_dir, 'oracle_fetchMany_test.txt'))
        self.oracle_db.save_to_excel(data, os.path.join(output_dir, 'oracle_fetchMany_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchMany_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchMany_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'oracle_fetchMany_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, 'oracle_fetchMany_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, 'oracle_fetchMany_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, 'oracle_fetchMany_test.xlsx'), data)

    def test_postgres_fetchMany_save_to_files(self):
        """Test Postgres DB features: save to CSV, TXT, Excel."""
        self.postgres_db.execute_file('./Test/SQL Files/sample.sql')
        data = self.postgres_db.fetchmany(3)

        # Save to files
        output_dir = './Test/TestFiles'
        self.postgres_db.save_to_csv(data, os.path.join(output_dir, 'postgres_fetchMany_test.csv'))
        self.postgres_db.save_to_txt(data, os.path.join(output_dir, 'postgres_fetchMany_test.txt'))
        self.postgres_db.save_to_excel(data, os.path.join(output_dir, 'postgres_fetchMany_test.xlsx'))

        # Check if files exist
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchMany_test.csv')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchMany_test.txt')))
        self.assertTrue(os.path.exists(os.path.join(output_dir, 'postgres_fetchMany_test.xlsx')))

        # Verify file contents
        self.verify_csv_content(os.path.join(output_dir, 'postgres_fetchMany_test.csv'), data)
        self.verify_txt_content(os.path.join(output_dir, 'postgres_fetchMany_test.txt'), data)
        self.verify_excel_content(os.path.join(output_dir, 'postgres_fetchMany_test.xlsx'), data)
    
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
        """Tear down the test environment: drop the test tables."""
        
        # Dropping Oracle table
        try:
            cls.oracle_db.execute_query("DROP TABLE TestActors CASCADE CONSTRAINTS")
            cls.oracle_db.execute_query("COMMIT")  # Issue COMMIT separately after the DROP statement
        except Exception as e:
            print(f"Error dropping Oracle table: {e}")
        finally:
            cls.oracle_db.close()

        # Dropping Postgres table
        try:
            cls.postgres_db.execute_query("DROP TABLE IF EXISTS TestActors CASCADE")
            cls.postgres_db.execute_query("COMMIT")  # Issue COMMIT separately
        except Exception as e:
            print(f"Error dropping Postgres table: {e}")
        finally:
            cls.postgres_db.close()

        # Remove the entire TestOutput directory
        output_dir = './Test/TestFiles/'
        if os.path.exists(output_dir):
            shutil.rmtree(output_dir)

if __name__ == '__main__':
    unittest.main()
