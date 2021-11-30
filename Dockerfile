FROM prioreg.azurecr.io/prio-data/uvicorn_deployment:2.0.0

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

COPY ./queryset_manager/ /queryset_manager

ENV GUNICORN_APP="queryset_manager.app:app"
