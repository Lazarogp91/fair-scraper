FROM mcr.microsoft.com/playwright/python:v1.50.0-jammy

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copia el código (main.py + easyfairs_widgets.py + lo que haya)
COPY . .

ENV PYTHONUNBUFFERED=1

CMD ["sh", "-c", "uvicorn main:app --host 0.0.0.0 --port ${PORT:-8000}"]
