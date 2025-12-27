#!/bin/sh
echo "서버를 시작합니다..."
cd email-management-system && . venv/bin/activate && streamlit run app.py --server.port 14626 &
echo $! > server.pid

# 서버가 시작될 시간을 잠시 줍니다.
sleep 3

echo ""
echo "========================================="
echo "서버가 정상적으로 실행되었습니다."
echo "아래 URL로 접속하여 확인해주세요:"
echo "http://narnia-lab.duckdns.org:14626"
echo "========================================="
