# Use official Python slim image
FROM python:3.10-slim-buster

# Set working directory
WORKDIR /app

# Copy project files
COPY . /app

# Remove leftover .egg-info, upgrade pip, then install requirements
RUN find . -name "*.egg-info" -exec rm -rf {} + \
    && pip install --upgrade pip \
    && pip install -r requirements.txt


# Expose port
EXPOSE 5000

# Run the application
CMD ["python3", "app.py"]
