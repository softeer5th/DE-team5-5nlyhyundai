FROM public.ecr.aws/lambda/python:3.9

COPY . ${LAMBDA_TASK_ROOT}

RUN pip install --upgrade pip
RUN pip install -r requests_requirements.txt

CMD ["bobaedream_lambda_search.lambda_handler"]
# override CMD. This is the entry point of the lambda function.