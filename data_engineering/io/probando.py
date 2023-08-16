from pde_dagster.register.logger import logger
import os
from glob import glob

logger.info('ola k ase')


ruta_archivo = glob(os.path.join(os.path.dirname(__file__), "*.json"))
creds_path = "".join(ruta_archivo)

print(creds_path)