FROM python:3.11-slim

# Создаем рабочую директорию
WORKDIR /app

# Устанавливаем зависимости OS (чтобы requests и ssl работали стабильно)
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Копируем файлы
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py .

# Переменные окружения (можно, но не обязательно)
ENV PYTHONUNBUFFERED=1

CMD ["python", "main.py"]