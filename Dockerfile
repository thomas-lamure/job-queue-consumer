FROM python:3.8

RUN pip3 install azure-storage-queue

ADD run.py /

CMD ["python3", "/run.py"]
