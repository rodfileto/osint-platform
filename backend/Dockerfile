# Use an official Python runtime as a parent image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /app

# Install dependencies
COPY requirements.txt /app/
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copy the current directory contents into the container at /app
COPY . /app/
RUN chmod +x /app/entrypoint.prod.sh

# Expose port 8000
EXPOSE 8000

# Run the application (default command, can be overridden in docker-compose)
CMD ["/app/entrypoint.prod.sh"]
