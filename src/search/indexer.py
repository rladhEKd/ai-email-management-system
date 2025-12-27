import sys
import os
import shutil
import numpy as np
import pickle
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, DATETIME, ID
from src.ingestion.parser import parse_eml_files
from sentence_transformers import SentenceTransformer

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
        
        # 1. Whoosh 색인 생성
        schema = self._create_schema()
        if os.path.exists(self.index_dir):
            shutil.rmtree(self.index_dir)
        os.makedirs(self.index_dir)

        ix = create_in(self.index_dir, schema)
        writer = ix.writer()

        email_count = 0
        texts_to_embed = []
        doc_ids = []
        
        try:
            print("Whoosh 색인을 생성하는 중...")
            for email_obj in parse_eml_files(self.eml_dir):
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
                
                # 시맨틱 검색을 위한 텍스트와 ID 저장
                text_content = f"{email_obj.subject if email_obj.subject else ''}\n{email_obj.body_plain if email_obj.body_plain else ''}"
                texts_to_embed.append(text_content)
                doc_ids.append(email_obj.message_id)
                
                email_count += 1

            writer.commit()
            print(f"{email_count}개의 이메일이 Whoosh 색인에 성공적으로 추가되었습니다.")
        except Exception as e:
            print(f"이메일 색인 중 오류가 발생했습니다: {e}")
            writer.cancel()
            return

        # 2. 시맨틱 검색을 위한 임베딩 생성
        try:
            print("\n시맨틱 검색을 위한 임베딩 벡터를 생성합니다...")
            print("이 작업은 모델 다운로드를 포함하여 몇 분 정도 소요될 수 있습니다.")
            # 다국어 지원 모델 로드
            model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            
            embeddings = model.encode(texts_to_embed, show_progress_bar=True)
            
            # 임베딩과 문서 ID 저장
            embedding_path = os.path.join(self.index_dir, '..', 'embeddings.npy')
            doc_id_path = os.path.join(self.index_dir, '..', 'doc_ids.pkl')
            
            np.save(embedding_path, embeddings)
            with open(doc_id_path, 'wb') as f:
                pickle.dump(doc_ids, f)
                
            print(f"임베딩 벡터가 '{embedding_path}'에 저장되었습니다.")
            print(f"문서 ID가 '{doc_id_path}'에 저장되었습니다.")
            
        except Exception as e:
            print(f"임베딩 생성 또는 저장 중 오류가 발생했습니다: {e}")


if __name__ == '__main__':
    print("===== Whoosh 검색 색인 및 시맨틱 임베딩 구축 시작 =====")
    indexer = EmailIndexer()
    indexer.index_emails()
    print("===== 모든 색인 작업 완료 =====")