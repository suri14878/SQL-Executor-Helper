import logging, os, configparser, datetime

# function to create config file for logger
def CreateLoggerConfig(config_filename = "./Configs/Logger.ini"):
    try:
        print(f"Attempting to create Logger config file '{config_filename}'...")

        # create config object
        config = configparser.ConfigParser(allow_no_value=True)
        config.optionxform = str

        # Adds new section and settings for logger behavior
        config["Logger Settings"]={
            "# Save path for the log file, the log file name, and the log file extension": None,
            "FilePath": "./Logs/",
            "FileName" : "Log File",
            "Extension": ".log",
            "# Whether or not to include a timestamp in the log file name. Value of TRUE includes, FALSE excludes.": None,
            "IncludeTimestamp" : "FALSE",
            "# If not using timestamps, log files will go to a single file. This option controls if want the file to be overwritten each run. Appends data if not.": None,
            "Overwrite" : "TRUE",
            "# Sets whether logfile should be sent to console as well as log file": None,
            "ConsoleOutput" : "TRUE",
            '# Log level defines behavior of logging file and which messages are included.': None,
            '# DEBUG - Detailed information, typically of interest only when diagnosing problems.': None,
            '# INFO - Confirmation that things are working as expected.': None,
            '# WARNING - An indication that something unexpected happened, or indicative of some problem in the near future (e.g. ‘disk space low’). The software is still working as expected.': None,
            '# ERROR - Due to a more serious problem, the software has not been able to perform some function.': None,
            '# CRITICAL - A serious error, indicating that the program itself may be unable to continue running.': None,
            "LogLevel" : "DEBUG",
            }
        
        # Make directory and save file
        MakeDirectory(config_filename)
        with open(config_filename, 'w') as configfileObj:
            config.write(configfileObj)
            configfileObj.flush()
            configfileObj.close()

        print(f"Config file '{config_filename}' created.")
    except Exception as e:
        print(f"Exception creating logger config file: {e}")

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

# function to read configurations file
def ReadLoggerConfig(file_path = "./Configs/Logger.ini"):
    try:
        config = configparser.ConfigParser()
        config.read(file_path)
        return config
    except Exception as e:
        return None
    
# function to create a logger
def create_logger():
    config = ReadLoggerConfig()
    file_path = config['Logger Settings']['FilePath']
    file_name = config['Logger Settings']['FileName']
    extension = config['Logger Settings']['Extension']
    include_timestamp = config['Logger Settings']['IncludeTimestamp']
    overwrite = config['Logger Settings']['Overwrite']
    consoleOutput = config['Logger Settings']['ConsoleOutput']
    log_level = config['Logger Settings']['LogLevel']

    # Creates directory for logger if it doesn't exist
    MakeDirectory(file_path)
    
    if(include_timestamp == 'TRUE'):
        now = datetime.now()
        log_file = file_path + file_name + now.strftime(r" %m.%d.%Y %H.%M.%S") + extension
    else:
        log_file = file_path + file_name + extension
       
    handlers = [logging.FileHandler(log_file, mode=f"{'w' if (overwrite=='TRUE') else 'a'}")]
    if(consoleOutput == 'TRUE'):
        handlers.append(logging.StreamHandler())

    logging.basicConfig(level=log_level, 
                        handlers=handlers,
                        format=r'%(asctime)s [%(levelname)s] [%(name)s]: %(message)s', 
                        datefmt=r'%m/%d/%Y %I:%M:%S %p'
                        )
    return logging