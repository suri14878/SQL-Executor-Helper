# SQL-Executor-Helper
This project will help to interact with both Postgres and Oracle Databases. This will serve as the primary access pattern/system to submit SQL queries.

### Installation with git credentials (easiest):
To include the project, add the following line to your requirements.txt. You will have to be logged into your git account on the host machine.
```
Executor @ git+https://github.com/suri14878/SQL-Executor-Helper@main
```
### Installation without git credentials
1. Clone the repository:
   ```git clone https://github.com/suri14878/SQL-Executor-Helper.git```

2. Set up a Python virtual environment. For windows, there is a `Create Virtual Environment.bat` file within the `Batch Scripts` subfolder that will automatically set up the virtual environment and all dependencies.
3. Build the package using `Create_Python_Package.bat`file within the `Batch Scripts` subfolder that will automatically builds the package. It will drop the tar.gz and wheel file into a dist subfolder. Alternatively, you call the following two commands from the project directory.
```
 py -m pip install --upgrade build
 py -m build
```
4. The below example references saving the file to a 'Packages' project subfolderin your project. This would be the line you place in your requirements.txt.
```
./Packages/executor-0.0.1.tar.gz
```

## How to Unit Test the project
1. Make sure you have already created virtual environment, If not you can setup by running `Create Virtual Environment.bat` file within the `Batch Scripts` subfolder.
2. Set up the default configurations. Run the `Create Configs.py` file in the `Setup` folder. This will create a configs subfolder and configuration files for the logger.
3. Make sure that you have correct configs for database in `Configs/Database_Config.ini`.
4. Run the `Test/Unit Tests.py` file.
5. Alternatively, there's a `Basic Connection.py` file that can be used for one-off queries.

## Programatic Usage

### Basics (Connection & Select Statements)
1.  **Connecting to Databases**: You can easily connect to either Postgres or Oracle databases using the following commands:  
   
    For Oracle:
    
    ```python
    oracle_db = SQLExecutor(OracleConnection(), config_file='./Configs/Database_Config.ini', environment='test')
    ```
    For Postgres:
    ``` python
    postgres_db = SQLExecutor(PostgresConnection(), config_file='./Configs/Database_Config.ini', environment='test')
    ```

2.  **Executing a File and Saving the Result**: To execute a SQL file and save the result (whether it's a single query or a multi-query file):

    ```python
      db.execute_file_and_save(file_name, result_file_path, result_file_type=FileType.CSV)
      ```

    Note: `FileType` is an enum that supports the following formats: CSV, TXT, and Excel.

    **Saving Logic**:

    -   If the file contains multiple queries, each query will be saved in a separate file, with an index number appended to the result_file_path.

3.  **Executing a Folder and Saving Results**: To execute all SQL files inside a folder and save the results:

    ```python
      db.execute_folder_and_save(folder_path, result_save_path, result_file_type=FileType.CSV)
      ```

    **Saving Logic**:

    -   The function opens and executes each file in the folder.
    -   For each query in each file, a separate result file is created. The result file name will follow the original file name from the folder, with the index number of the queries appended.

4.  **Additional Parameters**: Both execute_file_and_save and execute_folder_and_save allow additional parameters like batch_size and row_limit. These parameters apply to all queries in the given file.

    You can also use specific comments to control pagination and row limits for individual queries:

    -   **Pagination**: Add the following comment above a query to paginate the result:

        ```SQL
         /* PAGINATE SIZE 2 */
        SELECT * FROM SampleTable;
         ```

    -   **Row Limit**: Add the following comment above a query to limit the number of rows returned:

       ```SQL 
         /* ROW LIMIT 10 */
        SELECT * FROM SampleTable;
      ```

    You can combine both comments for more control, and they will only apply to the query directly below them. The batch_size and row_limit parameters passed to the functions will not affect these queries.

### Advanced Usage
These will use server-side cursors to fetch the results, By default oracle has server-side cursors.

1.  You can get queries by file:
     ```python
      queries = SQLExecutor.get_queries_from_file('filename.sql')
      ```
2.  You can get query by index from either a file or an already read string object:
      ```python
      queries = SQLExecutor.get_queries_from_file('filename.sql', index=1)
      queries = SQLExecutor.get_queries_from_str(read_query_str, index=1)
      ```
3.  You can get query by name from either a file or an already read string object:
      ```python
      queries = SQLExecutor.get_queries_from_file('filename.sql', name="QueryName")
      queries = SQLExecutor.get_queries_from_str(read_query_str, name="QueryName")
      ```

    Name must be defined in the query as an additional parameter.
       ```SQL 
         /* NAME QueryName */
        SELECT * FROM SampleTable;
      ```

4. You can get rows by batches and save them to specified file type:
      ```python
        batches = db.get_batches_by_query(query, page_size=3)
        db.save_results_in_batches(batches, 'test', result_file_type=FileType.EXCEL)
        for i, batch in enumerate(batches):
            db.save_to_csv(batch, 'test', is_append= True if i!=0 else False, include_header=True if i==0 else False)
      ```
      You can use these additional parameters:
      * `is_append= True` it is used to append content, default set to False.
      * `include_header=True` it is used to put headers, default set to TRUE.
      * `delimiter=','` it is used to specify delimiter when using the CSV file type, default set to ','.
      * `apply_limit=None` it is used to apply row limit, default set to None.
      * `apply_batch_size=None` it is used to specify batch size, default set to None.

      You can also pass parameters like below.  
      - **For Oracle:**
      ```python
        lang ='English'
        batches  = oracle_db.get_batches_by_query(f"SELECT * FROM MOVIES WHERE LANGUAGE = :lang", page_size=5, params=[lang])
        rows = [row for row in itertools.chain.from_iterable(batches)]
        print(rows)
      ```
      For more details (Oracle): https://python-oracledb.readthedocs.io/en/latest/user_guide/bind.html  
      - **For Postgres:**
      ```python
        lang ='English'
        batches  = oracle_db.get_batches_by_query(f"SELECT * FROM MOVIES WHERE LANGUAGE = %s", page_size=5, params=[lang])
        rows = [row for row in itertools.chain.from_iterable(batches)]
        print(rows)
      ```
      For more details (Postgres): https://www.psycopg.org/psycopg3/docs/basic/params.html
5. There is a mapping method where we can map the fetched results into a list of provide class object. Below is the example of how you can use it.
   ```python
   # This is a example of EventInstance class.
   class EventInstance:
    def __init__(self, instance_index=None, definition_index=None, instance_period=None, 
                 lifecycle_stage=None, instance_status=None, instance_note=None):
        self.instance_index = instance_index
        self.definition_index = definition_index
        self.instance_period = instance_period
        self.lifecycle_stage = lifecycle_stage
        self.instance_status = instance_status
        self.instance_note = instance_note

   # Here I'm passing query along with EventInstance class to the method whill will yeild batches of (list of class objects) for a given page_size.
   batch_instances = postgres_db.map_rows_to_objects('SELECT * FROM event_tracker.event_instances', EventInstance, page_size=5)
   for instances in batch_instances:
       for instance in instances:
           print(instance.lifecycle_stage)
   ```
### Data Manipulation Language (DML) commands

```python
@retry_transaction(default_args={
    'db': db,
    'config_file': config_file,
    'environment': environment
})
def insert_queries(db):
    with db.transaction():
    	# This function just executes all the queries from the file.
        db.execute_file('InsertQueriesFile.sql')

    with db.transaction():
    	# This function just takes query and execute
        db.execute_query('''INSERT INTO SAMPLE_TABLE(Name, Age, Dob, Bio)
        VALUES (val1, val2, val3, val4);''')
        db.execute_query('''INSERT INTO SAMPLE_TABLE(Name, Age, Dob, Bio)
        VALUES (val1, val2, val3, val4);''')
        db.execute_query('''INSERT INTO SAMPLE_TABLE(Name, Age, Dob, Bio)
        VALUES (val1, val2, val3, val4);''')
        db.execute_query('''INSERT INTO SAMPLE_TABLE(Name, Age, Dob, Bio)
        VALUES (val1, val2, val3, val4);''')
        
insert_queries()
```
**Note:** Both the functions are not for fetching.

1. In the above example, all the `INSERT` queries are wrapped within the `with db.transaction()` block, which acts as a context manager. This ensures that all changes are committed at the end of the block, and any error triggers an automatic rollback.
2. To handle database connection failures, you can use the `@retry_transaction()` decorator from SQL Executor. This decorator will automatically retry the database connection if it fails and re-execute the `insert_queries` function. For this to work, the `@retry_transaction()` decorator must have default_args dictionary  that has `db`,`config_file` and `environment`.
3. Additionally, you can pass custom parameters to `@retry_transaction()`, such as:
   * `tries:` The number of attempts before giving up.
   * `delay:` The initial delay between attempts, in seconds.
   * `backoff:` A multiplier that increases the delay after each retry.  
**For example:** `@retry_transaction(default_args, tries=3, delay=3, backoff=2)`.
