# SQL-Executor-Helper

## Introduction
The SQL Executor Helper project will help to interact with both Postgres and Oracle Databases. This will serve as the primary access pattern/system to submit.

## How to Install the Project
Below are the steps to install and execute the SQL-Executor-Helper project:

1. Clone the repository:
   ```git clone https://github.com/ULL-IR-Office/SQL-Executor-Helper.git```

3. Set up a Python virtual environment. For windows, there is a `Create Virtual Environment.bat` file within the `Batch Scripts` subfolder that will automatically set up the virtual environment and all dependencies.

4. Set up the default configurations. Run the `Create Configs.py` file in the `Setup` folder. This will create a configs subfolder and configuration files for the logger.

## How to Test the project
1. Put any sample query in `Test/SQL Files/sample.sql` file.
2. Make sure that you have correct configs for database in `Configs/Database_Config.ini`.
3. Run the `Test/Test.py` file.

## How to Utilize the Project

1.  **Connecting to Databases**: You can easily connect to either Postgres or Oracle databases using the following commands:  
   
    For Oracle:
    
    ```bash
    oracle_db = SQLExecutor(OracleConnection(), config_file='./Configs/Database_Config.ini', environment='test')
    ```
    For Postgres:
    ``` bash
    postgres_db = SQLExecutor(PostgresConnection(), config_file='./Configs/Database_Config.ini', environment='test')
    ```


2.  **Executing a File and Saving the Result**: To execute a SQL file and save the result (whether it's a single query or a multi-query file):

    ```bash
      db.execute_file_and_save(file_name, result_file_path, result_file_type=FileType.CSV)
      ```

    `FileType` is an enum that supports the following formats: CSV, TXT, and Excel.

    **Saving Logic**:

    -   If the file contains multiple queries, each query will be saved in a separate file, with an index number appended to the result_file_path.
3.  **Executing a Folder and Saving Results**: To execute all SQL files inside a folder and save the results:

    ```bash
      db.execute_folder_and_save(folder_path, result_save_path, result_file_type=FileType.CSV)
      ```

    **Saving Logic**:

    -   The function opens and executes each file in the folder.
    -   For each query in each file, a separate result file is created. The result file name will follow the original file name from the folder, with the index number of the queries appended.
4.  **Additional Parameters**: Both execute_file_and_save and execute_folder_and_save allow additional parameters like batch_size and row_limit. These parameters apply to all queries in the given file.

    You can also use specific comments to control pagination and row limits for individual queries:

    -   **Pagination**: Add the following comment above a query to paginate the result:

        ```bash
         /* PAGINATE SIZE 2 */
        SELECT * FROM SampleTable;
         ```

    -   **Row Limit**: Add the following comment above a query to limit the number of rows returned:

       ```bash 
         /* ROW LIMIT 10 */
        SELECT * FROM SampleTable;
      ```

    You can combine both comments for more control, and they will only apply to the query directly below them. The batch_size and row_limit parameters passed to the functions will not affect these queries.'

## How to use it Programmatically

1.  You can get queries by file:
     ```bash
      queries = db.get_queries_from_file('filename.sql')
      ```
2.  You can get query by index:
      ```bash
      queries = db.get_query_by_index('filename.sql', index=1)
      ```
3. You can get rows by batches and save them to specified file type:
      ```bash
      batches = oracle_db.get_batches_by_query(query, page_size=3)
      for i, row in enumerate(batches):
          oracle_db.save_to_csv(row, 'test', is_append= True if i!=0 else False, include_header=True if i==0 else False)
      ```