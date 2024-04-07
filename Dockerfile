# Use the official Python base image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /forecrypt

# Copy the requirements file to the container
COPY requirements.txt .

# Install the Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . .

# Define an environment variable for the port
ENV PORT=8000

# Expose the port that your FastAPI app will be running on
EXPOSE $PORT

# Start the FastAPI app
CMD uvicorn app:app --host 0.0.0.0 --port $PORT