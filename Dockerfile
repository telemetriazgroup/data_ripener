# apiztrackok - FastAPI con MongoDB en la nube
FROM python:3.12-slim

WORKDIR /app

# Dependencias del sistema que pueden hacer falta para psycopg2/cryptography
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Instalar dependencias: se excluye "cdc" (no existe en PyPI y no se usa en el código)
COPY requirements.txt .
RUN grep -v '^[[:space:]]*cdc[[:space:]]*$' requirements.txt > requirements.docker.txt \
    && pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.docker.txt

# Código de la aplicación
COPY ./app ./app

# La app corre con uvicorn; el módulo FastAPI es server.app:app (desde app/)
WORKDIR /app/app
EXPOSE 9055

# Sin --reload en contenedor (producción)
#CMD ["uvicorn", "server.app:app", "--host", "0.0.0.0", "--port", "9055"]
CMD ["uvicorn", "server.app:app", "--host", "0.0.0.0", "--port", "9055", "--reload"]