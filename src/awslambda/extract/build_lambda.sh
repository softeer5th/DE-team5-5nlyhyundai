aws ecr get-login-password --region ap-northeast-2 | docker login --username AWS --password-stdin 181252290322.dkr.ecr.ap-northeast-2.amazonaws.com
docker buildx build --platform linux/amd64 --provenance=false --push -f requests.Dockerfile -t 181252290322.dkr.ecr.ap-northeast-2.amazonaws.com/lambda_crawler:requests-test .
docker buildx build --platform linux/amd64 --provenance=false -f selenium.Dockerfile --push -t 181252290322.dkr.ecr.ap-northeast-2.amazonaws.com/lambda_crawler:selenium .

