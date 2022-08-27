ARG FUNCTION_DIR="/home/app/"

FROM python:slim-buster

ARG FUNCTION_DIR

# Setup for ssh onto github
RUN mkdir -p ${FUNCTION_DIR}
COPY *.py ${FUNCTION_DIR}

# Installing Lambda image dependencies
RUN apt-get update \
#  && apt-get install -y \
  && apt-get install default-jdk -y \
  g++ \
  make \
  cmake \
  unzip \
  libcurl4-openssl-dev

RUN python3 -m pip install pyspark \
  && python3 -m pip install pandas

# Installing mysqldump and cleaning apt cache
RUN apt update && apt install -y mariadb-client && \
  apt-get clean autoclean && \
  apt-get autoremove --yes && \
  rm -rf /var/lib/{apt,dpkg,cache,log}/

WORKDIR ${FUNCTION_DIR}


#docker build -t img-rokt-system .
#docker run --rm -it -v $(pwd):/home/app/ --entrypoint bash img-rokt-system
