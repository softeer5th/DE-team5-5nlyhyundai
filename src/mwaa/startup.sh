#!/bin/sh

echo "Printing Apache Airflow component"
echo $MWAA_AIRFLOW_COMPONENT

# 한 dag 내 최대 task 수
export AIRFLOW__CORE__DAG_CONCURRENCY=100
# 한 dag 내 최대 동시 실행 수
export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16
# 전체 시스템에서 동시 실행 가능한 task 수
export AIRFLOW__CORE__PARALLELISM=32

#core.default_task_retries 3
#email.email_backend
#smtp.smtp_host 
#smtp.smtp_starttls
#smtp.smtp_ssl
#smtp.smtp_port
#smtp.smtp_mail_from
