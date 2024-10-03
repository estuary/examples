FROM postgres:16

RUN apt-get update && \
    apt-get install -y \
    postgresql-16-cron \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set the default command to run PostgreSQL
CMD ["postgres"]
