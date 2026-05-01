FROM python:3.12-slim-bookworm

# PGDG client major must be >= server major (Bookworm's default client is 15).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl gnupg \
    && install -d /usr/share/postgresql-common/pgdg \
    && curl -fsSL -o /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
        https://www.postgresql.org/media/keys/ACCC4CF8.asc \
    && gpg --dearmor -o /etc/apt/trusted.gpg.d/postgresql.gpg \
        /usr/share/postgresql-common/pgdg/apt.postgresql.org.asc \
    && echo "deb [signed-by=/etc/apt/trusted.gpg.d/postgresql.gpg] http://apt.postgresql.org/pub/repos/apt bookworm-pgdg main" \
        > /etc/apt/sources.list.d/pgdg.list \
    && apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client-17 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY pg_backup.py /app/pg_backup.py

# Config is supplied at runtime (bind-mount); see README.
ENV BACKUP_CONFIG=/config/backup.conf.toml

CMD ["python3", "/app/pg_backup.py"]
