FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PORT=8000 \
    DB_PATH=/data/app.db

WORKDIR /srv
COPY app/ ./app/
COPY tests/ ./tests/
COPY verify.py ./

RUN python -m compileall -q app tests verify.py

EXPOSE 8000
CMD ["python", "-m", "app.server"]
