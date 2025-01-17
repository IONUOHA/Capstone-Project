from apache/airflow:2.10.4

# copy requirements.txt file to the container
copy requirements.txt /requirements.txt

#upgrade pip
run pip install --upgrade pip

## install libraries
run pip install --no-cache-dir -r /requirements.txt