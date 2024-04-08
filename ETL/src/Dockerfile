FROM python:3.8.0
WORKDIR /ETL
RUN apt-get update
RUN apt-get install -y openjdk-11-jre-headless
COPY requirements.txt .
RUN python -m pip install -r requirements.txt
CMD ["python", "/ETL/src/app.py"]