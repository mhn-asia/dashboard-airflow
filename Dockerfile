FROM apache/airflow:latest

USER root

# Create missing directory
RUN mkdir -p /var/lib/apt/lists/partial

# Update the package list and install necessary dependencies
RUN apt-get update && apt-get install -y python3 cron gcc nano

# Install Python 3, pip, and libpq-dev for PostgreSQL support
RUN apt-get install -y python3-pip && apt-get install -y libpq-dev

# Set the timezone to Asia/Kuala_Lumpur
ENV TZ=Asia/Kuala_Lumpur
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y tzdata && \
    ln -snf /usr/share/zoneinfo/$TZ /etc/localtime


USER airflow
# Install psycopg2 for PostgreSQL database connectivity
RUN pip install psycopg2

# Install colorama and python-telegram-bot
RUN pip3 install colorama
RUN pip3 install python-telegram-bot==13.15



# Copy requirements.txt to the image
COPY requirements.txt .

# Install Python dependencies from requirements.txt
RUN pip3 install -r requirements.txt

# Install Python dependencies
RUN pip install -r requirements.txt
