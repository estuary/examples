FROM mcr.microsoft.com/mssql/server:2022-latest

# Switch to root to create the directory and copy the script
USER root

# Create directory for initialization scripts
RUN mkdir -p /var/opt/mssql/scripts

# Copy the initialization script
COPY init.sql /var/opt/mssql/scripts/init.sql

# Switch back to mssql user
USER mssql

# Set the entrypoint to run the SQL Server and initialization script
CMD /bin/bash -c '/opt/mssql/bin/sqlservr & sleep 30 && /opt/mssql-tools/bin/sqlcmd -S localhost -U sa -P $SA_PASSWORD -i /var/opt/mssql/scripts/init.sql && tail -f /dev/null'
