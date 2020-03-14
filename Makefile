TAG=puckel/docker-airflow
CONTAINER=airflow-scheduler-webserver

start:
	docker run --rm -d -p 8088:8080 -v `pwd`/dags/:/usr/local/airflow/dags --name=$(CONTAINER) $(TAG) webserver

stop:
	docker stop $(CONTAINER)

