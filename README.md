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
4. Put any sample query in `Test/SQL Files/sample.sql` file.
5. Make sure that you have correct configs for database in `Configs/Database_Config.ini`.
6. Run the `Test/Test.py` file.
