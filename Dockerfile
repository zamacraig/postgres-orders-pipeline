FROM python:3.12-slim

WORKDIR /app

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy pipeline source and data
COPY pipeline/ ./pipeline/
COPY data/      ./data/

WORKDIR /app/pipeline

CMD ["python", "main.py"]
