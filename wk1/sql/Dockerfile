# Use python version 3.9
FROM python:3.9

# Install Linux app wget
RUN apt-get install wget
# Install python packages
RUN pip install pandas sqlalchemy psycopg2

# Set working diretory to /app
WORKDIR /app
# copy ingest_data.py to the working directory in the Docker container
COPY ingest_data.py ingest_data.py

# Set the entrypoint
ENTRYPOINT [ "python", "ingest_data.py" ]