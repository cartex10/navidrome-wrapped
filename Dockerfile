FROM python:3.11

# RUN pip install requests

COPY *.py /
COPY .env /
VOLUME /db

CMD python main.py