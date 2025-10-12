FROM python:3.12-slim
WORKDIR /app

# Systemdeps nur wenn nötig (hier meist nicht) – Beispiel:
# RUN apt-get update && apt-get install -y --no-install-recommends gcc && rm -rf /var/lib/apt/lists/*
COPY docker/requirements-producer.txt .
RUN pip install --no-cache-dir -r requirements-producer.txt

# Projektcode (alles) ins Image (für Dev kannst du weiterhin mounten, s.u.)
COPY . .

ENV PYTHONUNBUFFERED=1 PIP_DISABLE_PIP_VERSION_CHECK=1 PIP_ROOT_USER_ACTION=ignore
CMD ["python", "kafka/producer.py"]
