FROM python:3.9
COPY requirements.txt ./
RUN pip install -r requirements.txt
COPY . ./
CMD ["python", "data_processing/beam_pipeline.py"]