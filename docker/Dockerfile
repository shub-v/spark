# Define build arguments
ARG IMAGE_VARIANT=slim-buster
ARG OPENJDK_VERSION=8
ARG PYTHON_VERSION=3.9.8

# Stage 1: Install dependencies and set up Python environment
FROM python:${PYTHON_VERSION}-${IMAGE_VARIANT} AS py3

# Set working directory
WORKDIR /app

# Install curl
RUN apt-get update && apt-get install -y curl

# Copy requirements file
COPY requirements.txt /app/

# Install Python dependencies using pip
RUN pip install --no-cache-dir -r requirements.txt

# Download SQLite JDBC driver
RUN curl -L -o /app/sqlite-jdbc.jar https://github.com/xerial/sqlite-jdbc/releases/download/3.36.0.3/sqlite-jdbc-3.36.0.3.jar


# Install necessary tools, including curl, procps for ps, and sqlite3
RUN apt-get update && apt-get install -y curl procps sqlite3


# Stage 2: Set up the final image with OpenJDK and Python
FROM openjdk:${OPENJDK_VERSION}-${IMAGE_VARIANT}

# Copy Python and dependencies from the first stage
COPY --from=py3 / /

# Set environment variable for Spark to locate the SQLite JDBC driver
ENV SPARK_CLASSPATH="/app/sqlite-jdbc.jar"

# Set working directory
WORKDIR /app

# Copy the source code
COPY src /app/src

# Set the entry point to run pipeline.py
ENTRYPOINT ["python", "src/test.py"]
