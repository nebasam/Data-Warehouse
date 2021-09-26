FROM python:3.8
RUN pip install 'apache-airflow[postgres]==2.1.4' && pip install dbt
RUN pip install apache-airflow-providers-postgres
RUN pip install apache-airflow-providers-mysql
RUN pip install dbt-mysql
RUN pip install mysql-connector-python