FROM python:3.9-slim
ENV ENV=dev
RUN apt-get update

WORKDIR app
COPY . .
RUN pip install -r requirements.txt

CMD ["python", "-m", "src.producer"]
