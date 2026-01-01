# 1. Use an official Python runtime as a parent image
FROM python:3.12-slim

# 2. Set environment variables
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PORT=7860

# 3. Set the working directory in the container
WORKDIR /app

# 4. Install system dependencies (needed for some ML/Data packages)
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

# 5. Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 6. Copy the rest of the application code
COPY . .

# 7. Create a non-root user for security (Hugging Face best practice)
RUN useradd -m -u 1000 user
USER user
ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH

# 8. Set the working directory to where the code lives
WORKDIR $HOME/app
COPY --chown=user . $HOME/app

# 9. Expose the port Hugging Face Spaces expects
EXPOSE 7860

# 10. Run the application
# Replace 'app.main:app' with your actual FastAPI entry point
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "7860"]