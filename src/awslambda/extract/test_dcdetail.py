from dcmotors_lambda_detail import lambda_handler

def main():
    event = {}
    context = {}
    lambda_handler(event, context)
    