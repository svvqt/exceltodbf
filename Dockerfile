FROM python:3.13-slim

RUN mkdir /app

COPY requirements.txt /app

RUN python -m pip install --upgrade pip && pip3 install -r app/requirements.txt --no-cache-dir

COPY . /app

WORKDIR /app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]