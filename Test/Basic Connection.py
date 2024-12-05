import os,sys

parent_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
sys.path.insert(0, parent_dir)
import Logger
from Executor.SQLExecutor import SQLExecutor, OracleConnection

logging = Logger.create_root()
logger = logging.getLogger('Basic Connection Testing')

config_file = './Configs/Database_Config.ini'
query = "select 1 from dual"
environment = 'prod'
database = SQLExecutor(OracleConnection(), config_file=config_file, environment=environment)

batches = database.get_batches_by_query(query, page_size=3)
for i, batch in enumerate(batches):
    print(batch)
    # database.save_to_csv(batch, 'test', is_append= True if i!=0 else False, include_header=True if i==0 else False)

