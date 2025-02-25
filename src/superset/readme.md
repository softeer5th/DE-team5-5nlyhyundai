# Superset

Superset은 오픈소스 데이터 시각화 도구입니다. 이번 프로젝트에서는 슈퍼셋(Superset)을 이용하여 처리한 데이터를 시각화 하고, 경고 기준에 부합하면 이메일로 알림을 보내기도 합니다.

## 설정하기

### EC2 설정하기

시각화 툴은 항상 접근할 수 있어야 하기에 AWS EC2를 설정하고 설정한 EC2에 설치합니다.

우선 AWS EC2를 설정합니다. 수퍼셋을 안정적으로 실행하기 위해 다음 설정을 권장합니다.

* Amazon Linux
* T3 Large
* 외부 인터넷이 연결된 VPC에 설치
* 외부 인터넷에서 8088번으로 접근할 수 있도록 보안그룹 설정
* (선택사항) 원할한 접속을 위해 ssh key 설정

### 슈퍼셋 설치하기

EC2에 슈퍼셋을 설치하는 흐름은 다음과 같습니다.

도커 설치 -> 도커 컴포즈 설치 -> 슈퍼셋 다운로드 -> 슈퍼셋 설정 -> 도커 컴포즈로 슈퍼셋 설치 및 실행

0. EC2 연결

터미널을 키고 cd, ls(윈도우는 dir)등의 명령어를 이용해 ssh키를 발급받은 디렉토리로 이동합니다.

해당 디렉토리에서 다음을 수행합니다.

```bash
chmod 400 "발급받은 키"
ssh -i "발급받은 키" ec2-user@[EC2 주소]
```

1. 도커 설치

도커를 설치합니다.
```bash
sudo amazon-linux-extras install docker
```

도커 서비스를 시작하고 EC2 부팅 시 자동으로 시작되게 설정합니다.
```bash
sudo service docker start
sudo systemctl enable docker
```

현재 사용자를 도커 그룹에 추가하여 sudo 명령어 없이도 도커 명령어를 실행할 수 있도록 합니다.
```bash
sudo usermod -aG docker $USER
newgrp docker
```

도커가 정상 설치 되었는지 버전을 확인합니다
```bash
docker --version
```

2. 도커 컴포즈 설치

다음 명령어로 도커 컴포즈를 설치합니다.
```bash
sudo curl -L "https://github.com/docker/compose/releases/latest/download/docker-compose-$(uname -s)-$(uname -m)" -o /usr/local/bin/docker-compose
sudo chmod +x /usr/local/bin/docker-compose
```

도커 컴포즈가 정상 설치 되었는지 버전을 확인합니다.
```bash
docker-compose --version
```

3. 슈퍼셋 

슈퍼셋을 다운로드 받기 위해 wget을 설치합니다.
```bash
sudo yum install wget
```
슈퍼셋 깃허브 문서를 확인합니다.

[https://github.com/apache/superset](https://github.com/apache/superset)

녹색 우상단 버튼(<> Code)을 누른 후 Download ZIP 링크를 마우스 우클릭 해서 복사합니다.

그리고 다음과 같이 wget을 이용해서 EC2에 리포지토리를 다운로드 받습니다.
다음은 예시입니다.
```bash
wget https://github.com/apache/superset/archive/refs/heads/master.zip
```

다운로드가 끝난 파일은 압축을 해제합니다.
```bash
unzip master.zip
ls
```

그리고 나서 압축이 풀린 superset 디렉토리에 접근합니다.
```bash
cd superset-master
```

4. 슈퍼셋 설정

해당 설정을 건너뛰고 바로 5. 도커 컴포즈로 슈퍼셋 설치 및 실행으로 가도 좋습니다. 다만, 4번에서 하고자 하는 것은 이메일 알림을 위해 슈퍼셋 설정을 설치 전에 수정하는 것입니다.

```bash
vim ./docker/pythonpath_dev/superset_config.py
```
vim 등의 문서 작업 툴로 다음 파일을 수정합니다.
* ./docker/pythonpath_dev/superset_config.py

  45번 줄 근방에 다음을 추가합니다.
```python
SMTP_HOST = 'smtp.gmail.com' # Gmail로 메일을 보낼 경우입니다.
SMTP_STARTTLS = True    # TLS 프로토콜을 사용합니다
SMTP_SSL = False        # SSL 프로토콜은 사용하지 않습니다.
SMTP_USER = '당신의 gmail 주소'
SMTP_PORT = 587         # TLS 기본 프로토콜입니다.
SMTP_PASSWORD = '구글 앱 비밀번호를 발급받아 사용합니다.'
SMTP_MAIL_FROM = '당신의 gmail 주소'
```
  _구글 앱 비밀번호 사이의 공백은 ascii 문자가 아니라서 그대로 복사 붙여넣기 하면 ascii 문제 코덱 에러 뜹니다. visual code 등에 붙여넣기 해서 공백 문자를 지우고 다시 스페이스바로 ascii 공백문자를 추가해주시기 바랍니다._

* 다음을 부분을 수정하거나 추가합니다

```python
FEATURE_FLAGS = {"ALERT_REPORTS": True, "DATE_FORMAT_IN_EMAIL_SUBJECT":True}    # 109번 줄 근방
ALERT_REPORTS_NOTIFICATION_DRY_RUN = False   # 110번 줄 근방
WEBDRIVER_BASEURL = "http://(EC2 도메인):8088/"  # 111번 줄 근방
WEBDRIVER_TYPE = "chrome"   # 116번줄 근방에 추가
WEBDRIVER_OPTION_ARGS = [
    "--force-device-scale-factor=2.0",
    "--high-dpi-support=2.0",
    "--headless",
    "--disable-gpu",
    "--disable-dev-shm-usage",
    "--no-sandbox",
    "--disable-setuid-sandbox",
    "--disable-extensions",
]   # 윗 줄에 이어서 추가
```
위의 같은 파일 이곳 저곳에 퍼져 있습니다. 잘 찾아서 수정해주시기 바랍니다.

도커파일 182줄 부근에 크로미움 드라이버 설치 코드도 삽입해 줍니다.
```bash
vim Dockerfile
```
```Dockerfile
RUN apt-get update && apt-get install -y \
    unzip \
    wget \
    libgconf-2-4 \
    && wget https://storage.googleapis.com/chrome-for-testing-public/133.0.6943.53/linux64/chromedriver-linux64.zip \
    && unzip chromedriver-linux64.zip \
    && mv chromedriver-linux64/chromedriver /usr/local/bin/ \
    && chmod +x /usr/local/bin/chromedriver \
    && rm -rf chromedriver-linux64.zip chromedriver-linux64 \
    && apt-get clean && rm -rf /var/lib/apt/lists/*
```

5. 도커 컴포즈로 슈퍼셋 설치 및 실행

수퍼셋 이미지를 빌드합니다.
```bash
docker-compose -f docker-compose-non-dev.yml build
```

수퍼셋 이미지를 켭니다.
```bash
docker-compose -f docker-compose-non-dev.yml up -d
```

맨 마지막에 오는 -d는 선택사항입니다. 슈퍼셋 로그를 라이브로 보고싶다면 -d 옵션을 붙이지 않아도 됩니다.

_슈퍼셋의 초기 아이디 비밀번호는 admin/admin입니다. 보안을 위해 로그인 후 settings -> List Users에서 유저 추가하고 관리자 비밀번호를 바꾸시길 바랍니다_

참고로 레드 쉬프트를 종료하는 방법은 다음과 같습니다.

* -d 옵션을 붙이지 않았다면, "ctrl + C" 입력으로 슈퍼셋을 종료할 수 있습니다.
* -d 옵션을 붙이고 실행했다면 다음 명령어를 입력하여 슈퍼셋 종료가 가능합니다.
```bash
docker-compose -f docker-compse-non-dev.yml down
```

### 슈퍼셋과 데이터 베이스 연결하기

슈퍼셋과 데이터 베이스를 연결해야 합니다. 이번 프로젝트에서는 슈퍼셋을 레드쉬프트와 연결하여 사용합니다.

이때, DB URL은 AWS Redshift 콘솔에 나와있는 워킹그룹(working group) 엔드포인트(end-poin)의 포트 번호 앞(URL 마지막 콜론의 좌측까지) 까지입니다.

포트번호는 같은 엔드포인트의 포트번호(URL 마지막 콜론 우측 숫자)입니다.

데이터 베이스 이름은 포트번호 우측 슬래쉬(/)이후 부분입니다. 보통 dev입니다.

ID와 비밀번호는 레드쉬프트에서 설정해 두었던 것으로 이용합니다.


### 슈퍼셋 대시보드 탬플릿 설정

해당 디렉토리에 있는 압축파일을 슈퍼셋에 업로드 합니다.
