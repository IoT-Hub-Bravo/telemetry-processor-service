# Базовий образ
FROM python:3.11-slim

# Env
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Робоча директорія
WORKDIR /app

# Системні залежності
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Копіюємо залежності та встановлюємо
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Копіюємо весь проект
COPY . .

# Запуск FastAPI через uvicorn
CMD ["uvicorn", "src.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]