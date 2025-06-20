import os
from pymongo import MongoClient
from pymongo.errors import BulkWriteError

def inserir_dados(dados, batch_size=1000):
   try:
       uri = os.getenv('MONGO_URI')
       db_name = os.getenv('MONGO_DB')
       collection_name = os.getenv('MONGO_COLLECTION')

       client = MongoClient(uri)
       db = client[db_name]
       collection = db[collection_name]

       for i in range(0, len(dados), batch_size):
           batch = dados[i:i + batch_size]
           try:
               collection.insert_many(batch)
               print(f'Inseridos {len(batch)} documentos ({i + len(batch)}/{len(dados)})')
           except BulkWriteError as bwe:
               print(f'Erro de escrita em lote: {bwe.details}')
   except Exception as e:
       print(f'Erro ao salvar no MongoDB: {str(e)}')