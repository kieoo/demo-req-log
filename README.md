安装<br>
pip install -r requirements.txt <br>
运行<br>
python3.7 -Bm demo.bin.demo_request<br>

docker run<br>
docker run -d -v /tmp/req_demo:/tmp/req_demo --network=host -v /etc/localtime:/etc/localtime -v /root/.aws:/root/.aws  demo-crontab:v1.0 