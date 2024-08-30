FROM python:3.12-slim
ENV ENV='dev'
COPY . .
RUN pip install -r requirements.txt
RUN chmod a+x run.sh

