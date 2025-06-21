from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import JSONResponse
from dotenv import load_dotenv
import tempfile
from service.csv_service import processar_csv
from repository.mongo_repository import inserir_dados
from util.env_validator import validar_env_vars

load_dotenv()
validar_env_vars()

app = FastAPI()

@app.post("/upload-csv")
async def upload_csv(file: UploadFile = File(...)):
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Arquivo precisa ser CSV.")

    try:
        # Salva temporariamente
        temp = tempfile.NamedTemporaryFile(delete=False, suffix='.csv')
        contents = await file.read()
        temp.write(contents)
        temp.close()

        dados = processar_csv(temp.name)
        if not dados:
            raise HTTPException(status_code=500, detail="Erro ao processar o CSV.")

        inserir_dados(dados)

        return JSONResponse(content={"message": f"{len(dados)} registros inseridos com sucesso."})

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
