FROM python:alpine

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

RUN adduser -D celery
USER celery

COPY . .

CMD celery -A main worker --loglevel=INFO & celery -A main beat --loglevel=INFO