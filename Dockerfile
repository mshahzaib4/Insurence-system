# Use official Python slim image
FROM python:3.10-slim-buster

# Prevent Python from writing .pyc files and buffering stdout/stderr
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# Set working directory
WORKDIR /app

# Install system dependencies needed for building some Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libffi-dev \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*

# Copy only requirements first for caching
COPY requirements.txt /app/requirements.txt

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# Copy project files
COPY . /app

# Remove leftover .egg-info if any
RUN find . -name "*.egg-info" -exec rm -rf {} +

# Expose port
EXPOSE 5000

# Run the application
CMD ["python3", "app.py"]
