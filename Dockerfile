FROM python:3.12
ENV PATH="/root/.local/bin:${PATH}"

WORKDIR /src
COPY requirements.txt .
COPY requirements-test.txt .
RUN pip install --user -r requirements.txt -r requirements-test.txt
