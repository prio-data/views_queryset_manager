FROM curlimages/curl:latest as fetch-cert
USER root
RUN curl https://cacerts.digicert.com/DigiCertGlobalRootG2.crt.pem --output /root.crt

FROM python:3.8
RUN sed 's/SECLEVEL=[0-9]/SECLEVEL=1/g' /etc/ssl/openssl.cnf > /etc/ssl/openssl.cnf

COPY ./requirements.txt /
RUN pip install -r requirements.txt 

ENV PRODUCTION=1

COPY --from=fetch-cert /root.crt /.postgresql/root.crt

COPY ./queryset_manager/ /queryset_manager
CMD ["gunicorn","-k","uvicorn.workers.UvicornWorker","--bind","0.0.0.0:80","queryset_manager.app:app"]
