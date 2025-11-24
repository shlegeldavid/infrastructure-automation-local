FROM python:3.11-slim

WORKDIR /app

# Устанавливаем зависимости для Pillow (нужны библиотеки для обработки изображений)
RUN apt-get update && apt-get install -y \
    libjpeg-dev \
    zlib1g-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем requirements и устанавливаем зависимости
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копируем сам скрипт
COPY main.py .

# Создаем директорию для данных
RUN mkdir -p /app/data

# Запускаем скрипт
CMD ["python", "main.py"]
