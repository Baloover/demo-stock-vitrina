FROM python:3.10-slim

EXPOSE 8501

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

COPY ../requirements.freeze.txt .
RUN pip install -r requirements.freeze.txt \
      --no-cache-dir --progress-bar off

WORKDIR /usr/src/app
COPY . .

ENTRYPOINT ["streamlit", "run", "main.py", "--server.address=0.0.0.0"]