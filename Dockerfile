# Use uma imagem base com Python
FROM python:3.9-slim

# Instale as dependências do sistema
RUN apt-get update && apt-get install -y \
    wget \
    gnupg2 \
    unzip \
    chromium \
    chromium-driver \
    && rm -rf /var/lib/apt/lists/*

# Instale o pip e outras dependências do Python
RUN pip install --upgrade pip

# Defina o diretório de trabalho
WORKDIR /app

# Copie os arquivos do projeto
COPY app/ /app

# Instale as dependências do Python
RUN pip install --no-cache-dir -r requirements.txt

# Exponha a porta (se necessário)
EXPOSE 8888

# Set environment variable for Chrome
ENV CHROME_PATH=/usr/bin/chromium

# Comando de execução
CMD ["python", "/app/main.py"]