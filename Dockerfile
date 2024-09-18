# Use an official Python runtime as a base image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app

# Install any necessary dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Install Gunicorn
RUN pip install gunicorn

# Expose port 5000 for Gunicorn
EXPOSE 8081

# Run Gunicorn as the WSGI server
CMD ["gunicorn", "-w","4","-b", "0.0.0.0:8081", "app:app"]
