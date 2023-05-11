FROM python:3.8

COPY requirements.txt .

RUN pip3 install -r requirements.txt

COPY model_server model_server
COPY saved_models model_server/static/
COPY run.py .

ENV FLASK_RUN_HOST=0.0.0.0
ENV FLASK_APP=model_server:app

EXPOSE $PORT

CMD ["python", "run.py"]

# CMD [ "python3", "-m" , "flask", "run", "--host=0.0.0.0"]

