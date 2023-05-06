FROM public.ecr.aws/lambda/python:3.8

COPY get_data_lambda/ .

CMD ["get_data_lambda.lambda_handler"] 
