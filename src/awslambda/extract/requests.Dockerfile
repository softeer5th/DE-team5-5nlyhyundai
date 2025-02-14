FROM public.ecr.aws/lambda/python:3.9

COPY . ${LAMBDA_TASK_ROOT}

RUN pip install --upgrade pip
RUN pip install -r requests_requirements.txt

CMD ["bobaedream.lambda_search.lambda_handler"]