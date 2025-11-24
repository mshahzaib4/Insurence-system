FROM python:3.10-slim-buster AS builder

WORKDIR /app

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install --prefix=/install --no-cache-dir -r requirements.txt

FROM python:3.10-slim-buster
WORKDIR /app

COPY --from=builder /install /usr/local
COPY . .

EXPOSE 5000
CMD ["python3", "app.py"]
