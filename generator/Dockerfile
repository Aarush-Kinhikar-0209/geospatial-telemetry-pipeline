# ---------------------------------------------------------------------------
# generator / Dockerfile
# Container image for the synthetic fleet telemetry data producer.
# ---------------------------------------------------------------------------

FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies.
COPY generator/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code.
COPY generator/main.py .

# Run the generator by default.
CMD ["python", "main.py"]
