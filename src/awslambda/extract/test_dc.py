from dcmotors_lambda_detail_test import lambda_handler

def main():
    event = {}
    context = {}
    lambda_handler(event, context)

main()