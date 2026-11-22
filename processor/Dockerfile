# ---------------------------------------------------------------------------
# processor / Dockerfile
# Container image for the stream processing application.
# ---------------------------------------------------------------------------

FROM python:3.12-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1

# Install Python dependencies.
COPY processor/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
COPY processor/main.py .

# Run the processor by default.
CMD ["python", "main.py"]
