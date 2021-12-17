FROM python:3.7.12-alpine3.15
MAINTAINER qingyue.ke <qingyue.ke@mobvista.com>

WORKDIR /data/wwwroot
COPY . .
RUN cp cronjobs /etc/crontabs/root && pip install -r requirements.txt

CMD ["crond", "-f", "-d", "8"]