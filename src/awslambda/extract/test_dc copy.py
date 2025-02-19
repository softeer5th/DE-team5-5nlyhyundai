from dcmotors_lambda_search import lambda_handler

def main():
    event = {'checked_at' : '2025-02-14 05:00:00+00:00', 'keyword' : '벤츠'}
    context = {}
    lambda_handler(event, context)

main()