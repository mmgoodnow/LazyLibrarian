FROM python:latest

# Install python modules we need
RUN pip install mysql-connector-python configparser bottle pyyaml bash

# Telemetryserver listens to this post
EXPOSE 9174/tcp

# Copy all the files we need to /app
COPY *.py /app/
COPY tslogging.yaml /app/
COPY telemetry.ini /app/

# Run the telemetry server
WORKDIR "/app"
CMD ["python3", "/app/TelemetryServer.py"]

