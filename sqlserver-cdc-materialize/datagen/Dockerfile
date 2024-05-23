FROM python:3.12-bullseye

RUN apt-get update && apt-get install -y \
    curl \
    gnupg \
    apt-transport-https \
    ca-certificates \
    unixodbc unixodbc-dev libgss3 odbcinst

# Add Microsoft SQL Server ODBC Driver 17 for Linux
RUN curl https://packages.microsoft.com/keys/microsoft.asc | tee /etc/apt/trusted.gpg.d/microsoft.asc \
  && curl https://packages.microsoft.com/config/debian/11/prod.list | tee /etc/apt/sources.list.d/mssql-release.list \
  && apt-get update \
  && ACCEPT_EULA=Y apt-get install msodbcsql18 --assume-yes \
  && ACCEPT_EULA=Y apt-get install -y mssql-tools18

# Copy and install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# Copy application files
COPY . .

# Set the command to run the application
CMD ["python", "-u", "datagen.py"]
