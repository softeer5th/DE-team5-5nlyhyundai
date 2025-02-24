# Batch Transform

EMR을 이용해서 배치 한 주기에 나온 데이터의 변환을 수행합니다.

## 사용방법

1. AWS EMR을 설정합니다.

* Application bundle을 Spark Interaction으로 설정합니다.

* EMR Cluster를 위해 다음 권한이 필요합니다.
  > * EMR
  >   * EC2 접근권한
  >   * S3 접근권한
  > * EC2s for EMR
  >   * EMR EC2 Role
  >   * S3 접근권한

2. EMR 실행 결과와 EMR 실행 코드를 저장할 버켓을 설정합니다.

3. scripts/transform에 있는 EMR-2.py 파일을 S3에 업로드 합니다.
```bash
aws s3 cp emr_realtime.py s3://transform-emr
```

4. 업로드 된 소스코드는 에어플로우(Airflow)에 의해 때가 되면 자동으로 실행됩니다.

## 실행 로직

EMR에서는 스파크(Spark)로 다음 처리를 진행합니다:

* S3에서 Extractor가 작성한 Parquet 읽어옵니다.

* 게시글 및 댓글 중 주제에 대해 긍정적인 내용을 가진 작성물의 수를 셉니다.
* 게시글 및 댓글 중 주제에 대해 중립적인 내용을 가진 작성물의 수를 셉니다.
* 게시글 및 댓글 중 주제에 대해 부정적인 내용을 가진 작성물의 수를 셉니다.
  > 각 게시물의 감정분석 결과는 Extract 단계에서 LLM 모델을 통해 변환되어 parquet에 작성되어 있습니다.
* 집계된 데이터를 parquet 파일로 S3에 저장합니다.
* S3에 분산되어 저장된 게시글 및 댓글을 UNION하여 parquet 파일로 S3에 저장합니다.

사고, 결함, 경쟁사, 경쟁 모델을 키워드로 지정하고 위 연산을 진행하여 이들이 얼마나 언급이 되는지 연산합니다.
