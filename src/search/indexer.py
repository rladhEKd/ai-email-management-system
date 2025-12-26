import sys
import os

# Manually add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

import sqlite3
import json
from whoosh.index import create_in, open_dir
from whoosh.fields import Schema, TEXT, DATETIME, ID
from whoosh.qparser import QueryParser
from datetime import datetime
from src.ingestion.storage import SQLiteStorage

class EmailIndexer:
    def __init__(self, db_path="data/emails.db", index_dir="data/index"):
        self.db_path = db_path
        self.index_dir = index_dir
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)

    def _create_schema(self):
        return Schema(
            message_id=ID(stored=True, unique=True),
            subject=TEXT(stored=True),
            body_plain=TEXT(stored=True),
            sender=TEXT(stored=True),
            folder_path=TEXT(stored=True),
            receivers=TEXT(stored=True),
            sent_date=DATETIME(stored=True),
            thread_topic=TEXT(stored=True)
        )

    def index_emails(self):
        print(f"'{self.db_path}'에서 이메일 데이터를 로드 중...")
        storage = SQLiteStorage(self.db_path)
        storage.connect()
        if not storage.conn:
            print("오류: 데이터베이스 연결에 실패했습니다.")
            return

        schema = self._create_schema()
        # 기존 색인이 있으면 삭제하고 새로 생성
        if os.path.exists(self.index_dir):
            import shutil
            shutil.rmtree(self.index_dir)
            os.makedirs(self.index_dir)

        ix = create_in(self.index_dir, schema)
        writer = ix.writer()

        try:
            cursor = storage.conn.cursor()
            cursor.execute(
                "SELECT message_id, subject, body_plain, sender, receivers, sent_date, folder_path, thread_topic FROM emails;"
            )

            email_count = 0
            for row in cursor:
                sent_date_dt = datetime.fromisoformat(row[5]) if row[5] else None

                writer.add_document(
                    message_id=row[0],
                    subject=row[1] if row[1] else "",
                    body_plain=row[2] if row[2] else "",
                    sender=row[3] if row[3] else "",
                    receivers=row[4] if row[4] else "",
                    sent_date=sent_date_dt,
                    folder_path=row[6] if row[6] else "",
                    thread_topic=row[7] if row[7] else ""
                )
                email_count += 1

            writer.commit()
            print(f"{email_count}개의 이메일이 성공적으로 색인되었습니다.")
        except Exception as e:
            print(f"이메일 색인 중 오류가 발생했습니다: {e}")
            writer.cancel()
        finally:
            storage.close()

if __name__ == '__main__':
    print("===== Whoosh 검색 색인 구축 시작 =====")
    indexer = EmailIndexer()
    indexer.index_emails()
    print("===== Whoosh 검색 색인 구축 완료 =====")
