# ---------------------------------------------------------------------------
# api / Dockerfile
# Container image for the FastAPI serving layer.
# ---------------------------------------------------------------------------

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install Python dependencies.
COPY api/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
COPY api/main.py .

EXPOSE 8000

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
