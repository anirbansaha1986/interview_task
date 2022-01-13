FROM python:3.9

WORKDIR /usr/src/app
COPY . /usr/src/app/
RUN pip install --no-cache-dir -r requirements.txt

CMD ["uvicorn", "makeApiFast:app", "--host", "0.0.0.0", "--port", "8008"]