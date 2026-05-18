FROM python:3.11-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    flask==3.0.3 \
    flask-cors==4.0.1 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    gunicorn==22.0.0

COPY api.py .
COPY static/ ./static/

EXPOSE 3000

CMD ["gunicorn", "--bind", "0.0.0.0:3000", "--workers", "2", "--timeout", "30", "api:app"]
