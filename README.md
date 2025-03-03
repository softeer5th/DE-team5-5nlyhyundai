# DE-team5-5nlyhyundai
현대자동차그룹 소프티어 부트캠프 5기 데이터 엔지니어링 5조 5nlyHYUNDAI 입니다.

# 팀원
| 이름  | Github                        |
|------|-------------------------------|
| 김은태 | https://github.com/ket0825    |
| 신윤석 | https://github.com/tlsdbstjr  |
| 윤우석 | https://github.com/woosukYoon |

# 프로젝트 소개
커뮤니티 반응을 모니터링 하여 신차 출시 이후 브랜드 평가를 확인합니다. 5분단위 배치로 실시간에 가까운 결과를 제공합니다.
브랜드 이미지에 타격이 갈만한 수준의 커뮤니티 여론이 악화됨을 감지하면 사용자에게 알림 메일을 보내 즉각적으로 커뮤니티 여론을 인지할 수 있게 됩니다.

# 데이터 파이프라인
![프로젝트](https://github.com/softeer5th/DE-team5-5nlyhyundai/blob/main/readmeSrc/aws-5-2.png)
* [extract](https://github.com/softeer5th/DE-team5-5nlyhyundai/tree/main/src/awslambda/extract)

  디시인사이드, 클리앙, 보배드림 커뮤니티 텍스트 데이터를 크롤링 해 옵니다.
  Search 단계에서는 게시판 게시글 목록을 읽어와서 데이터 베이스에 저장합니다.
  Detail 단계에서는 데이터 베이스에 있는 게시글 정보를 읽어와 게시글 url에 직접 접근하여 데이터를 수집하고 저장합니다.
  Detail 단계에서 gpt api를 이용한 감성 분석도 함께 실시되고, 멀티 스레드를 통해 데이터 추출과 감성 분석을 병렬 처리할 수 있도록 합니다.

* [Transform](https://github.com/softeer5th/DE-team5-5nlyhyundai/tree/main/src/emr)

  배치처리 시작 시각(checked_at) 기반의 폴더명으로 분산 처리로 인해 쪼개진 모든 parquet 파일들을 불러오고, 이들을 합칩니다.
  추출한 데이터를 가지고 checked_at 기반의 연산을 진행합니다.
  사고, 결함, 경쟁사, 경쟁 모델을 키워드로 지정하여 검색어와 함께 이들이 얼마나 언급이 되는지 연산합니다.

* [Load](https://github.com/softeer5th/DE-team5-5nlyhyundai/blob/main/readmeSrc/redshiftReadMe.md)

  EMR을 통해 처리된 데이터를 S3를 거쳐 Redshift에 로드합니다. 게시글, 댓글, 그리고 checked_at을 기준으로 연산이 완료된 데이터를 Redshift에 post, comment, summary라는 이름으로 적재합니다.

* [Visualize](https://github.com/softeer5th/DE-team5-5nlyhyundai/tree/main/src/superset)

  Redshift와 SuperSet을 연동하여 시각화를 진행합니다.
  대시보드에는 실시간 탭과 종합 분석 탭이 존재합니다.
  실시간 탭에는 브랜드 및 신차 언급 지표, 부정 텍스트 지표를 추이를 보여줌으로써 차량의 이슈도를 트래킹하면서 부정적인 이슈를 감지하고 대처할 수 있도록 도와줍니다.
  종합 분석 탭에서는 1시간 단위 및 하루 단위로 종합적인 분석 지표를 제공하여, 단순 실시간 모니터링을 넘어 더 깊이 있는 인사이트를 도출할 수 있도록 구성하였습니다.

* [Airflow](https://github.com/softeer5th/DE-team5-5nlyhyundai/tree/main/src/mwaa)

  프로젝트 전체 과정을 관리합니다.
  일정 배치 주기(5분)마다 Extract, Transform, Load 과정을 호출하고, 이를 통해 데이터 파이프라인을 관리합니다.

* [RDS](https://github.com/softeer5th/DE-team5-5nlyhyundai/tree/main/src/rds)

  RDS는 extract 단계에서 크롤러를 관리합니다.
  

위에 나열된 각 요소의 설정을 마치면 5nly HYUNDAI 팀의 데이터 파이프라인을 구성할 수 있습니다.
