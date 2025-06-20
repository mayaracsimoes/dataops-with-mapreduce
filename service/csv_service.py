import csv
import sys
from util.encoding import detect_encoding

def processar_csv(csv_file_path, chunk_size=1000):
   try:
       encoding = detect_encoding(csv_file_path)
       print(f"Detectada codificação: {encoding}")

       with open(csv_file_path, 'r', encoding=encoding) as csv_file:
           try:
               dialect = csv.Sniffer().sniff(csv_file.read(1024))
               csv_file.seek(0)
           except:
               dialect = None

           csv_reader = csv.DictReader(csv_file, dialect=dialect)
           all_rows = []
           for i, row in enumerate(csv_reader):
               clean_row = {
                   k.strip(): v.encode('utf-8', 'ignore').decode('utf-8').strip()
                   if isinstance(v, str) else v
                   for k, v in row.items() if k
               }
               all_rows.append(clean_row)

               if (i + 1) % chunk_size == 0:
                   sys.stdout.write(f'\rProcessadas {i + 1} linhas...')
                   sys.stdout.flush()

       print(f'\nTotal de {len(all_rows)} linhas processadas.')
       return all_rows
   except Exception as e:
       print(f'Erro ao processar CSV: {str(e)}')
       return None