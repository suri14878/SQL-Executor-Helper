import os, configparser

# function to create config file for Test
def CreateTestConfig(config_filename = "./Configs/Test_Config.ini"):
    try:
        print(f"Attempting to create Test config file '{config_filename}'...")

        # create config object
        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str

        # Adds new sections for unit te
        config["TestSettings"]={
            "# Set to TRUE to enable testing for that database, or FALSE to disable it":None,
            "TestOracle" : "TRUE",
            "TestPostgres" : "TRUE",
            "# Set to TRUE to clean up test files after testing, or FALSE to keep them":None,
            "CleanUpFiles" : "TRUE"
        }


        # Make directory and save file
        MakeDirectory(config_filename)
        with open(config_filename, 'w') as configfileObj:
            config.write(configfileObj)
            configfileObj.flush()
            configfileObj.close()

        print(f"Config file '{config_filename}' created.")
    except Exception as e:
        print(f"Exception creating Test config file: {e}")

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