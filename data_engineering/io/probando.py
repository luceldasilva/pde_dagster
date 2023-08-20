from pde_dagster.register.logger import logger
'''import os
from glob import glob

logger.info('ola k ase')


ruta_archivo = glob(os.path.join(os.path.dirname(__file__), "*.json"))
creds_path = "".join(ruta_archivo)

print(creds_path)'''

'''table_name = 'hola'

start_timestamp = 'que'

end_timestamp = 'hace'

query = f"SELECT * FROM {table_name}"

query += (
        f" WHERE stripe_data ->> 'created' >= '{start_timestamp}'" 
        f" AND stripe_data ->> 'created' < '{end_timestamp}'"
    )

logger.info(query)

print(query)'''

error = 'sí anda XD'

logger.error(
    f'Ocurrió un error al descargar el archivo'
    f' desde Google Drive: {error}'
)