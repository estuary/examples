FROM python:3.11

COPY requirements.txt /tmp/requirements.txt

RUN pip install -r /tmp/requirements.txt

COPY . .

CMD ["python", "-u", "datagen.py"]
