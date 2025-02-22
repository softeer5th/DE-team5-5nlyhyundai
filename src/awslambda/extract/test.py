import requests
from bs4 import BeautifulSoup
headers = {
        "Accept":"text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
}
r = requests.get("http://www.clien.net/service/board/park/18776924?combine=true&q=%EB%B2%A4%EC%B8%A0&p=42&sort=recency&boardCd=&isBoard=false", headers=headers)
with open('clien.html', 'w', encoding='utf-8') as f:
    f.write(r.text)

html = None
with open('clien.html', 'r', encoding='utf-8') as f:
    html = f.read()

soup = BeautifulSoup(html, 'html.parser')
print("")
print("")
print("")
print("")
print("")