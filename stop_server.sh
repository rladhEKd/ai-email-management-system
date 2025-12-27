#!/bin/sh
if [ -f server.pid ]; then
    echo "PID 파일을 통해 서버를 중지합니다..."
    kill -9 $(cat server.pid) 2>/dev/null
    rm server.pid
fi

# 포트 기반으로 확실하게 종료 (PID 파일이 없거나 잘못된 경우 대비)
PID=$(lsof -t -i:14626)
if [ ! -z "$PID" ]; then
    echo "포트 14626 에서 실행 중인 프로세스($PID)를 강제 종료합니다..."
    kill -9 $PID
fi

echo "서버가 중지되었습니다."
