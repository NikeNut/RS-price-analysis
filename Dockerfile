FROM python:3.9

ADD RS_Pricing.py .

COPY ./requirements.txt /code/requirements.txt

RUN pip install --no-cache-dir --upgrade -r /code/requirements.txt

CMD ["python", "./RS_Pricing.py"]