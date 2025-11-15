FROM ubuntu:latest
LABEL authors="esak"

ENTRYPOINT ["top", "-b"]