FROM python:3.8-slim

COPY pyproject.toml pyproject.toml

COPY run.sh run.sh

RUN pip install poetry

RUN poetry config virtualenvs.create false

RUN poetry install --no-dev 

COPY ./src /src

ENTRYPOINT ["/run.sh"]
