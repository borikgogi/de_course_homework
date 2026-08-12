# Завдання 1 — Dockerfile для ingestor. Специфікація: SPEC.md → «Завдання 1».
#
# Один образ, одна стадія. Що має бути (деталі й бали — у SPEC.md):
#
#   * базовий образ python:3.12-slim з фіксованим тегом (не latest);
#   * PYTHONUNBUFFERED=1 — інакше логів завантаження не видно;
#   * non-root користувач (напр. appuser, uid 1000), USER — перед ENTRYPOINT;
#   * каталог /cache має належати цьому користувачу — туди монтується volume;
#   * залежності з app/requirements.txt ставляться ОКРЕМИМ шаром ПЕРЕД копіюванням
#     коду, з --no-cache-dir (правка коду не має перебудовувати pip install);
#   * ENTRYPOINT в exec-form запускає app/gh_ingest.py.
#
# Перевірте збірку:  docker build -t gh-ingest:latest .
# Скрипт app/gh_ingest.py — ДАНО, не редагуйте його.

# TODO: реалізуйте Dockerfile тут.

# Base with fixing tag
FROM python:3.12-slim

# No buffer logs
ENV PYTHONUNBUFFERED=1

# 3. Create non root user - appuser (UID 1000) and folder /cache
RUN adduser --disabled-password --gecos "" --uid 1000 appuser \
    && mkdir -p /cache \
    && chown -R appuser:appuser /cache

WORKDIR /workspace

# Copying requirements.txt for cashing pip
COPY app/requirements.txt ./app/requirements.txt
RUN pip install --no-cache-dir -r ./app/requirements.txt

# Copying python script
COPY app/ ./app/

# Access for work directory appuser
RUN chown -R appuser:appuser /workspace

# Selecting Upuser
USER appuser

# HEALTHCHECK
HEALTHCHECK --interval=30s --timeout=5s --start-period=5s --retries=3 \
    CMD python -c "import psycopg, requests" || exit 1

# ENTRYPOINT
ENTRYPOINT ["python", "app/gh_ingest.py"]
