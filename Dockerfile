FROM node:20-bookworm as base

ENV TZ="Asia/Jerusalem"

ARG PY_VERSION="3.11.0"

ENV HOME="/root"
WORKDIR ${HOME}
RUN apt-get update
RUN apt-get install -y git libbz2-dev libncurses-dev  libreadline-dev libffi-dev libssl-dev build-essential python3-dev
RUN git clone --depth=1 https://github.com/pyenv/pyenv.git .pyenv
ENV PYENV_ROOT="${HOME}/.pyenv"
ENV PATH="${PYENV_ROOT}/shims:${PYENV_ROOT}/bin:${PATH}"

RUN pyenv install $PY_VERSION
RUN pyenv global $PY_VERSION

WORKDIR /usr/src/app
COPY . .

RUN pip install -r requirements.txt

FROM base as prod

ENV OPREATION="all"
CMD python daliy_raw_dump.py ${OPREATION}

#

# RUN pip install -r requirements-dev.txt