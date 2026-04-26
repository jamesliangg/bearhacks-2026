FROM python:3.11-slim
WORKDIR /app
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

COPY services/common /opt/common
RUN pip install --no-cache-dir /opt/common

COPY services/ingestion/pyproject.toml ./pyproject.toml
COPY services/ingestion/app ./app
RUN pip install --no-cache-dir .

EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
