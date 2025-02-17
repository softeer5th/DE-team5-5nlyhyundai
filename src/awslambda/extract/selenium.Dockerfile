# ✅ Lambda Python 3.9 이미지 사용
FROM public.ecr.aws/lambda/python:3.9

# ✅ 필수 패키지 설치
RUN yum install -y \
    unzip \
    curl \
    wget \
    tar \
    xz \
    fontconfig \
    alsa-lib \
    atk \
    cups \
    gtk3 \
    libX11 \
    libXcomposite \
    libXcursor \
    libXdamage \
    libXext \
    libXi \
    libXrandr \
    libXScrnSaver \
    libXtst \
    mesa-libEGL \
    pango \
    && yum clean all

# ✅ Chrome & ChromeDriver 설치 후 `/opt/`에 저장
RUN mkdir -p /opt/chrome /opt/chrome-driver

RUN curl -sS -o /opt/chrome/chrome-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/133.0.6943.53/linux64/chrome-linux64.zip \
    && unzip /opt/chrome/chrome-linux64.zip -d /opt/chrome/ \
    && rm /opt/chrome/chrome-linux64.zip

RUN curl -sS -o /opt/chrome-driver/chromedriver-linux64.zip https://storage.googleapis.com/chrome-for-testing-public/133.0.6943.53/linux64/chromedriver-linux64.zip \
    && unzip /opt/chrome-driver/chromedriver-linux64.zip -d /opt/chrome-driver/ \
    && rm /opt/chrome-driver/chromedriver-linux64.zip

# ✅ 실행 권한 부여
RUN chmod +x /opt/chrome/chrome-linux64/chrome
RUN chmod +x /opt/chrome-driver/chromedriver-linux64/chromedriver

# ✅ 설치된 Chrome 및 ChromeDriver 확인
RUN /opt/chrome/chrome-linux64/chrome --version
RUN /opt/chrome-driver/chromedriver-linux64/chromedriver --version

# ✅ Python 패키지 설치
RUN pip install --upgrade pip
RUN pip install beautifulsoup4 requests pyarrow selenium psycopg2-binary boto3 python-dotenv

# ✅ Lambda 실행 코드 복사
COPY dcmotors_lambda_detail.py /var/task/
COPY common_utils.py /var/task/
COPY settings.py /var/task/
COPY .env.lambda /var/task/

# ✅ Lambda 실행 핸들러 설정
CMD ["dcmotors_lambda_detail.lambda_handler"]
