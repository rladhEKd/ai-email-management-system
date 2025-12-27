import sys
import os
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, DATETIME, ID
from src.ingestion.parser import parse_eml_files # Import the new EML parser

class EmailIndexer:
    def __init__(self, eml_dir="eml_output", index_dir="data/index"):
        self.eml_dir = eml_dir
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
        print(f"'{self.eml_dir}'에서 이메일 데이터를 로드하여 색인을 시작합니다...")
        
        schema = self._create_schema()
        if os.path.exists(self.index_dir):
            import shutil
            shutil.rmtree(self.index_dir)
            os.makedirs(self.index_dir)

        ix = create_in(self.index_dir, schema)
        writer = ix.writer()

        email_count = 0
        try:
            # Use the EML parser to get email objects
            for email_obj in parse_eml_files(self.eml_dir):
                # Convert receivers list to a comma-separated string for Whoosh
                receivers_str = ",".join(email_obj.receivers) if email_obj.receivers else ""
                
                writer.add_document(
                    message_id=email_obj.message_id,
                    subject=email_obj.subject if email_obj.subject else "",
                    body_plain=email_obj.body_plain if email_obj.body_plain else "",
                    sender=email_obj.sender if email_obj.sender else "",
                    receivers=receivers_str,
                    sent_date=email_obj.sent_date,
                    folder_path=email_obj.folder_path if email_obj.folder_path else "",
                    thread_topic=email_obj.thread_topic if email_obj.thread_topic else ""
                )
                email_count += 1

            writer.commit()
            print(f"{email_count}개의 이메일이 성공적으로 색인되었습니다.")
        except Exception as e:
            print(f"이메일 색인 중 오류가 발생했습니다: {e}")
            writer.cancel()

if __name__ == '__main__':
    print("===== Whoosh 검색 색인 구축 시작 (.eml 기반) =====")
    # The parser now reads from 'eml_output' by default
    indexer = EmailIndexer()
    indexer.index_emails()
    print("===== Whoosh 검색 색인 구축 완료 =====")