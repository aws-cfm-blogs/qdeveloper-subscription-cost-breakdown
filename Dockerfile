FROM python:3.9-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy your script
COPY . .

ENTRYPOINT ["python", "src/q-dev-subscription-cost-using-athena.py"]
