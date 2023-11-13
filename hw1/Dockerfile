FROM ubuntu:22.04

RUN apt update

RUN apt install -y \
    python3 \
    python3-pip \
    iproute2 

COPY requirements.txt /tmp/requirements.txt

RUN pip3 install -r /tmp/requirements.txt

WORKDIR /app

COPY . /app

CMD ["./test.sh"]
