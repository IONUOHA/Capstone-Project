FROM apache/airflow:2.10.4

#copy requirements.txt file to the container
COPY requirements.txt /requirements.txt

#upgrade pip
Run pip install --upgrade pip


## install libraries
Run pip install --no-cache-dir -r /requirements.txt






