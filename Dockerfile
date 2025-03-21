FROM node:20-bookworm as base

ENV TZ="Asia/Jerusalem"

ARG PY_VERSION="3.11.0"

ENV HOME="/root"
WORKDIR ${HOME}
RUN apt-get update && apt-get install -y git libbz2-dev libncurses-dev  libreadline-dev libffi-dev libssl-dev build-essential python3-dev
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

RUN pyenv install $PY_VERSION
RUN pyenv global $PY_VERSION

WORKDIR /usr/src/app
COPY requirements.txt .

RUN pip install -r requirements.txt

FROM base as data_processing
COPY . .

CMD python daily_raw_dump.py

# Serving
FROM base as serving
COPY remotes.py .
COPY api.py .
COPY access_layer.py .
COPY token_validator.py .
COPY response_models.py .
COPY utils.py .
CMD uvicorn api:app --host 0.0.0.0 --port 8000 --proxy-headers
# RUN pip install -r requirements-dev.txt