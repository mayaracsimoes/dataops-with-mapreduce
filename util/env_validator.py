import os
def validar_env_vars():
  required_vars = ['MONGO_URI', 'MONGO_DB', 'MONGO_COLLECTION']
  for var in required_vars:
      if not os.getenv(var):
          raise EnvironmentError(f'Variável de ambiente "{var}" não está definida no .env')