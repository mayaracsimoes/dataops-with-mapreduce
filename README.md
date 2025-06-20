# Data Ingestion API com FastAPI e MongoDB

    Esta aplicação utiliza uma API para upload de arquivos CSV e inserção dos dados em um banco MongoDB.

    ## 🛠 Requisitos

    - Python 3.8+
    - MongoDB rodando localmente ou na nuvem

    ## ⚙️ Instalação

    1. Clone o projeto

    2. Instale as dependências:
    ```bash
    pip install -r requirements.txt
    ```

    3. Configure o arquivo `.env`:
    ```env
    MONGO_URI=mongodb://localhost:27017
    MONGO_DB=nome_do_banco
    MONGO_COLLECTION=nome_da_colecao
    ```

    ## 🚀 Como executar

    Inicie a API com o Uvicorn:
    ```bash
    uvicorn api:app --reload
    ```

    Acesse: `http://localhost:8000/docs` para testar via Swagger UI.

    ## 📫 Como usar via Postman

    - Método: POST
    - URL: `http://localhost:8000/upload-csv`
    - Body > Form-Data:
        - Chave: `file` (tipo **File**)
        - Valor: Selecione um arquivo `.csv`

    ## 📁 Estrutura de diretórios

    - `api.py` – Endpoint de upload via FastAPI
    - `service/` – Processamento e limpeza de dados CSV
    - `repository/` – Inserção de dados no MongoDB
    - `utils/` – Utilitários como detecção de encoding e validação do .env
