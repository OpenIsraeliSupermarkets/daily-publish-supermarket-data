FROM node:20.19.6-trixie-slim as base

ENV TZ="Asia/Jerusalem"

ARG PY_VERSION="3.11.0"

ENV HOME="/root"
WORKDIR ${HOME}
RUN apt-get update && apt-get install -y git libbz2-dev libncurses-dev  libreadline-dev libffi-dev libssl-dev build-essential python3-dev curl
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

RUN pyenv install $PY_VERSION
RUN pyenv global $PY_VERSION

WORKDIR /usr/src/app
COPY requirements.txt .

RUN pip install -r requirements.txt

COPY requirements-dev.txt .
RUN pip install -r requirements-dev.txt

COPY . .

FROM base as dev
RUN pip install -r requirements-dev.txt

FROM dev as testing
CMD python system_tests/main.py && python -m pytest .

FROM base as data_processing
CMD python main.py

# Serving: api.py
FROM base as serving
CMD uvicorn api:app --host 0.0.0.0 --port 8000 --proxy-headers
