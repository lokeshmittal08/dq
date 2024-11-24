FROM python:3.9-slim-bullseye


WORKDIR /app

COPY . .

RUN pip install --no-cache-dir -r requirements.txt

#RUN sleep 100
#CMD ["python", "main.py"]
CMD ["sleep", "infinity"]

