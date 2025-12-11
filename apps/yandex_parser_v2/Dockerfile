FROM python:3.10-slim

ENV DEBIAN_FRONTEND=noninteractive
ENV DISPLAY=:99

# -------- СИСТЕМНЫЕ ЗАВИСИМОСТИ --------
RUN apt-get update && apt-get install -y \
    wget unzip gnupg xvfb x11vnc websockify novnc \
    libnss3 libgdk-pixbuf-2.0-0 libgtk-3-0 libxss1 libasound2 libxshmfence1 \
    libgbm1 libu2f-udev libvulkan1 libglib2.0-0 libdrm2 libxtst6 libxrandr2 \
    ca-certificates fonts-liberation supervisor \
    && rm -rf /var/lib/apt/lists/*

# -------- УСТАНОВКА Google Chrome (stable) --------
RUN wget -q https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb && \
    apt-get update && apt-get install -y ./google-chrome-stable_current_amd64.deb || true && \
    rm google-chrome-stable_current_amd64.deb && \
    rm -rf /var/lib/apt/lists/*

# -------- ChromeDriver под текущую версию Chrome --------
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}' | cut -d'.' -f1) && \
    DRIVER_VERSION=$(wget -qO- https://googlechromelabs.github.io/chrome-for-testing/LATEST_RELEASE_$CHROME_VERSION) && \
    wget -q https://storage.googleapis.com/chrome-for-testing-public/$DRIVER_VERSION/linux64/chromedriver-linux64.zip && \
    unzip -q chromedriver-linux64.zip && \
    mv chromedriver-linux64/chromedriver /usr/bin/chromedriver && \
    chmod +x /usr/bin/chromedriver && \
    rm -rf chromedriver-linux64 chromedriver-linux64.zip

# -------- Python зависимости --------
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# -------- Копируем код --------
WORKDIR /app
COPY . /app

# -------- Entrypoint скрипт --------
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

EXPOSE 5900 6080

ENTRYPOINT ["/entrypoint.sh"]