FROM views3/uvicorn-deployment:p39

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

COPY ./queryset_manager/ /queryset_manager

ENV GUNICORN_APP="queryset_manager.app:app"
