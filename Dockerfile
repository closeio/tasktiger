FROM circleci/python:3.6

WORKDIR /src
COPY requirements.txt .
COPY requirements-test.txt .
RUN pip install --user -r requirements.txt -r requirements-test.txt
