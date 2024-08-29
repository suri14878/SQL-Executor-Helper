import sys, os, importlib
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
import Module.Helpers.Logger as Logger
# ClientServerConfigs = importlib.import_module("Modules.Generate Client and Server Configs")

def CreateConfigs():
    try:
        Logger.CreateLoggerConfig()
        logging = Logger.create_logger()
        logger = logging.getLogger("Create Configs")
        logger.info("Sucessfully created logger and associated config.")
    except Exception as e:
        print(f"Exception when setting up logger and associated config: {e}")
    
    try:
        logger.info("Setting up other configs...")
        # ClientServerConfigs.CreateClientConfig()
        # ClientServerConfigs.CreateSeverConfig()
        logger.info("Finished setting up other configs. Be sure to modify config files to your needs!")
    except Exception as e:
        logger.error(f"Error while setting up configs: {e}")

if __name__ == "__main__":
    CreateConfigs()
