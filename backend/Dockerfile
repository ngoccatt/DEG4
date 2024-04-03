FROM python:3.8.0
WORKDIR /backend
RUN apt-get update
RUN apt-get install -y openjdk-11-jre-headless
COPY requirements.txt .
RUN python -m pip install -r requirements.txt
CMD ["python", "/backend/app.py"]