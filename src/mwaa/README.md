# MWAA

- 프로젝트 전체 과정을 관리합니다.

- 일정 배치 주기(5분)마다 Extract, Transform, Load 과정을 호출하고, 이를 통해 데이터 파이프라인을 관리합니다.

- test는 `dag/test` 폴더에 있으며, 과거 데이터를 가져오는 backfill을 위한 역할도 겸하고 있습니다.

## `dag/`

### `lambda_search_detail_s3.py` | Extract
  1. 환경 체크(DEV / PROD), checked_at 등 각종 시작 환경을 체크합니다.



  2. 서치 단계 람다 설정: 키워드에 따라 게시판에 분산 검색할 수 있도록 설정합니다.
   
      - Airflow 변수인 `keyword_set_name`을 파악하여 RDS에서 Hook으로 검색할 키워드들을 가져옵니다.
     
      - Airflow 변수: `LAMBDA_SEARCH_FUNC`를 읽어 호출할 람다 함수를 가져옵니다.
     
      - **호출할 람다 함수 * 키워드 수** 만큼 분산 람다 함수 환경을 설정합니다.



  3. 서치 단계 람다 실행: 설정된 람다 함수를 호출합니다.
   
      - Dynamic Task Mapping으로 각 키워드와 람다 함수에 맞게 Task를 생성합니다.
     
      - 비동기 호출, 동기 호출 중 선택할 수 있습니다.

      - 5분 배치에 맞게 boto client의 timeout을 2분으로 설정하며, **성공 여부와 관계없이** 모든 작업이 완료되면 다음 Task로 넘어갑니다.

      - 최종적으로 생성된 Task들을 실행하여 DB에 크롤링 리스트를 Upsert합니다.



  4. 디테일 단계 람다 설정: `LAMBDA_DETAIL_FUNC`를 읽어 분산 처리할 람다 함수를 가져옵니다.
  
     - 플랫폼별로 업데이트할 게시글의 수를 가져오기 위하여 RDS에서 Hook으로 게시글 수를 가져옵니다.

     - `DETAIL_BATCH_SIZE`를 읽어 게시글 수에 비례한 람다 함수 개수를 설정합니다 **ceil(플랫폼 별 게시글 수 / `DETAIL_BATCH_SIZE`)**

     - **라운드 로빈 방식**으로 플랫폼 별 람다 호출 수를 분배합니다(동시 실행 가능한 Task가 정해져 있기에 람다 함수를 분배합니다).



  5. 디테일 단계 람다 실행: 설정된 람다 함수를 호출합니다.
   
      - Dynamic Task Mapping으로 디테일 단계 람다 설정에 따라 Task를 생성합니다.

      - 2분 timeout으로 비동기 호출을 하며, 성공 여부와 관계없이 모든 작업이 완료되면 다음 Task로 넘어갑니다.

      - 최종적으로 생성된 Task들을 실행하여 S3에 크롤링 데이터를 **적재**합니다.



  6. EMR DAG 트리거: EMR DAG를 트리거합니다.
   
     - checked_at을 전달하여 저장된 S3 데이터를 EMR로 전달합니다.
  

---

### `emr_transform_dag.py` ㅣ Transform
 1. checked_at 검사: checked_at을 체크하여 해당 시간대의 S3 키값을 가져옵니다.

 2. EMR Step 오퍼레이터: EMR Step을 환경에 맞게 실행합니다.
    - 분산 처리로 인하여 쪼개진 파일을 Timeout 없이 불러오기 위해 s3a의 connection 관련 설정을 추가합니다.
    - 실행할 Job 스크립트 경로인 `JOB_SCRIPT_PATH` 를 읽어 EMR Step을 실행합니다.
    - 최종적으로 S3에 변환된 데이터가 적재됩니다.
 
 3. Redshift dag 트리거: Redshift DAG를 트리거합니다.
    - EMR에서 처리된 데이터를 Redshift에 적재하기 위해 checked_at을 전달합니다.

---

### `s3_redshift.py` | Load
  1. checked_at 검사: checked_at을 체크하여 해당 시간대의 S3 키값을 가져옵니다. 그리고 post와 comment, summary에 대한 key값을 생성합니다.

  2. S3 TO Redshift 오퍼레이터: Redshift에 S3 데이터를 적재합니다.
     - s3 key값을 읽어 Redshift에 적재합니다.
     - 최종적으로 EMR을 거쳐 변환된 데이터가 post, comment, summary 테이블에 적재됩니다.

---

`dag/test`: 과거 데이터를 가져오는 Backfill 용, 혹은 테스트 용도로 사용합니다.

---

`test/`: aws-mwaa-local-runner를 이용하여 MWAA 환경 구축 전 테스트를 진행했습니다.

---
`startup.sh`: MWAA 환경 구축 시 초기 설정을 위한 스크립트입니다.
```bash
    # 한 dag 내 최대 task 수
    export AIRFLOW__CORE__DAG_CONCURRENCY=100
    # 한 dag 내 최대 동시 실행 수
    export AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG=16 (CELERY WORKER 수애 따라 조정)
    # 전체 시스템에서 동시 실행 가능한 task 수
    export AIRFLOW__CORE__PARALLELISM=32
```

---

`requirements.txt`: MWAA 환경에서 사용할 라이브러리를 정의합니다.
- constraint로 사용 가능 라이브러리 버전을 고정하지만, 모두 설치하기에 시간이 많이 소요되어 주석처리하였습니다.
- 그 대신, 적혀있는 라이브러리 버전에 맞게 설치하도록 하였습니다.

 
 
