FROM jupyter/pyspark-notebook

COPY pyspark_script.py /app/pyspark_script.py
WORKDIR /app

CMD ["spark-submit", "pyspark_script.py"]
