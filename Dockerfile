FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    requests==2.32.3 \
    mysql-connector-python==9.2.0 \
    python-dotenv==1.1.0 \
    apscheduler==3.11.0 \
    pytz==2025.2 \
    pandas==2.2.3 \
    jinja2==3.1.6 \
    fastapi==0.116.1 \
    psycopg==3.2.10 \
    psycopg_binary==3.2.10 \
    uvicorn==0.35.0
RUN pip install python-snap7==1.3