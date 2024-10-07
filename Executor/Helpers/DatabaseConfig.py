import os, configparser

# function to create config file for Database
def CreateDatabaseConfig(config_filename = "./Configs/Database_Config.ini"):
    try:
        print(f"Attempting to create Database config file '{config_filename}'...")

        # create config object
        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str

        # Adds new sections for test and prod configs for Both Oracle and Postgres Databases
        config["test_postgres"]={
            "host" : "localhost",
            "port" : "5432",
            "user" : "postgres",
            "password" : "ull@123",
            "dbname" : "postgres"
            }

        config["prod_postgres"]={
            "host" : "prod_db_host",
            "port" : "5432",
            "user" : "prod_user",
            "password" : "prod_password",
            "dbname" : "prod_db",
            }

        config["test_oracle"]={
            "host" : "localhost",
            "port" : "1521",
            "user" : "SYSTEM",
            "password" : "oracle@123",
            "sid" : "xe",
            }

        config["prod_oracle"]={
            "host" : "prod_oracle_host",
            "port" : "1521",
            "user" : "prod_oracle_user",
            "password" : "prod_oracle_password",
            "service_name" : "PROD_SID",
            }
        
        # Make directory and save file
        MakeDirectory(config_filename)
        with open(config_filename, 'w') as configfileObj:
            config.write(configfileObj)
            configfileObj.flush()
            configfileObj.close()

        print(f"Config file '{config_filename}' created.")
    except Exception as e:
        print(f"Exception creating Database config file: {e}")

def MakeDirectory(file_path):
    try:
        directory = os.path.dirname(file_path)
        if not os.path.exists(directory) and len(directory) > 0:
            print(f"Directory {directory} did not exist, creating it.")
            os.makedirs(directory)
        return True
    except Exception as e:
        print(f"Exception creating directory: {e}")
        return False