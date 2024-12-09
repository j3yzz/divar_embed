FROM ubuntu:latest
LABEL authors="jeyz"

ENTRYPOINT ["top", "-b"]