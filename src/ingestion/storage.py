import sqlite3
import json
import os
from datetime import datetime
from src.common.models import Email # Email 클래스 임포트

class SQLiteStorage:
    def __init__(self, db_path):
        """
        데이터베이스 경로를 인자로 받아 초기화합니다.
        """
        # db_path가 디렉토리만 포함하는 경우, 파일 이름을 추가합니다.
        if os.path.isdir(db_path):
            db_path = os.path.join(db_path, "emails.db")
            
        self.db_path = db_path
        self.conn = None
        print(f"데이터베이스 경로가 '{self.db_path}'로 설정되었습니다.")

    def connect(self):
        """
        데이터베이스에 연결합니다.
        """
        try:
            self.conn = sqlite3.connect(self.db_path)
            print("데이터베이스에 성공적으로 연결되었습니다.")
        except sqlite3.Error as e:
            print(f"데이터베이스 연결 중 오류가 발생했습니다: {e}")
            self.conn = None

    def close(self):
        """
        데이터베이스 연결을 닫습니다.
        """
        if self.conn:
            self.conn.close()
            print("데이터베이스 연결이 닫혔습니다.")

    def create_table(self):
        """
        'emails' 테이블을 생성합니다. 테이블이 이미 존재하면 생성하지 않습니다.
        """
        if not self.conn:
            print("오류: 데이터베이스에 연결되지 않았습니다.")
            return

        create_table_sql = """
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            message_id TEXT NOT NULL,
            subject TEXT,
            body_plain TEXT,
            body_html TEXT,
            sender TEXT,
            receivers TEXT, -- JSON 배열 형태의 텍스트로 저장
            sent_date TIMESTAMP,
            folder_path TEXT,
            thread_topic TEXT,
            ingested_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        try:
            cursor = self.conn.cursor()
            cursor.execute(create_table_sql)
            # message_id에 대한 인덱스 생성 (검색 성능 향상)
            cursor.execute("CREATE INDEX IF NOT EXISTS idx_message_id ON emails (message_id);")
            self.conn.commit()
            print("'emails' 테이블이 성공적으로 준비되었습니다.")
        except sqlite3.Error as e:
            print(f"테이블 생성 중 오류가 발생했습니다: {e}")

    def insert_emails(self, emails):
        """
        Email 객체 리스트를 데이터베이스에 삽입합니다.
        """
        if not self.conn:
            print("오류: 데이터베이스에 연결되지 않았습니다.")
            return

        insert_sql = """
        INSERT INTO emails (
            message_id, subject, body_plain, body_html, sender,
            receivers, sent_date, folder_path, thread_topic
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        data_to_insert = []
        for email in emails:
            # datetime 객체를 ISO 8601 문자열로 변환하여 저장
            sent_date_str = email.sent_date.isoformat() if email.sent_date else None
            receivers_json = json.dumps(email.receivers) if email.receivers else "[]"
            data_to_insert.append((
                email.message_id,
                email.subject,
                email.body_plain,
                email.body_html,
                email.sender,
                receivers_json,
                sent_date_str, 
                email.folder_path,
                email.thread_topic
            ))
        
        try:
            cursor = self.conn.cursor()
            cursor.executemany(insert_sql, data_to_insert)
            self.conn.commit()
            print(f"{len(emails)}개의 이메일이 성공적으로 삽입되었습니다.")
        except sqlite3.Error as e:
            print(f"이메일 삽입 중 오류가 발생했습니다: {e}")

if __name__ == '__main__':
    # 이 스크립트를 직접 실행하면, 'data' 폴더에 DB를 생성하고 테이블을 만드는 테스트를 수행합니다.
    # (프로젝트 루트 폴더에서 실행: python3 -m src.ingestion.storage)
    
    # 'data' 폴더가 없으면 생성
    if not os.path.exists("data"):
        os.makedirs("data")

    # 기존 DB 파일 삭제 (테스트를 위해)
    if os.path.exists("data/emails.db"):
        os.remove("data/emails.db")
        print("기존 'data/emails.db' 파일 삭제 완료.")

    storage = SQLiteStorage("data/emails.db")
    storage.connect()
    storage.create_table()

    # 더미 이메일 데이터 생성 및 삽입 테스트
    dummy_emails = [
        Email(
            message_id="dummy_msg_1",
            subject="더미 이메일 1",
            body_plain="이것은 첫 번째 더미 이메일입니다.",
            body_html="<p>이것은 첫 번째 더미 이메일입니다.</p>",
            sender="dummy1@example.com",
            receivers=["recipient1@example.com", "recipient2@example.com"],
            sent_date=datetime(2025, 1, 1, 10, 0, 0),
            folder_path="/Dummy/Inbox",
            thread_topic="더미 스레드"
        ),
        Email(
            message_id="dummy_msg_2",
            subject="더미 이메일 2",
            body_plain="두 번째 더미 이메일입니다.",
            body_html="<p>두 번째 더미 이메일입니다.</p>",
            sender="dummy2@example.com",
            receivers=["recipient3@example.com"],
            sent_date=datetime(2025, 1, 2, 11, 30, 0),
            folder_path="/Dummy/Sent",
            thread_topic=None
        )
    ]
    
    storage.insert_emails(dummy_emails)

    # 데이터가 잘 들어갔는지 확인하는 코드 (선택 사항)
    cursor = storage.conn.cursor()
    cursor.execute("SELECT id, message_id, subject, sender, receivers, sent_date, folder_path FROM emails;")
    rows = cursor.fetchall()
    print("\n--- DB에서 삽입된 데이터 확인 ---")
    for row in rows:
        print(row)
    
    storage.close()