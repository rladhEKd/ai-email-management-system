import sys
import os
import shutil
import numpy as np
import pickle
from whoosh.index import create_in
from whoosh.fields import Schema, TEXT, DATETIME, ID
from src.ingestion.parser import parse_eml_files
from sentence_transformers import SentenceTransformer
import chromadb

class EmailIndexer:
    def __init__(self, eml_dir="eml_output", index_dir="data/index", chroma_dir="data/chroma"):
        self.eml_dir = eml_dir
        self.index_dir = index_dir
        self.chroma_dir = chroma_dir
        if not os.path.exists(self.index_dir):
            os.makedirs(self.index_dir)
        if not os.path.exists(self.chroma_dir):
            os.makedirs(self.chroma_dir)

        self.chroma_client = None
        self.chroma_collection = None
        self._init_chroma()

    def _init_chroma(self):
        try:
            self.chroma_client = chromadb.PersistentClient(path=self.chroma_dir)
            self.chroma_collection = self.chroma_client.get_or_create_collection(name="email_embeddings")
            print(f"ChromaDB 컬렉션 'email_embeddings' 초기화 완료. 저장 경로: {self.chroma_dir}")
        except Exception as e:
            print(f"ChromaDB 초기화 중 오류 발생: {e}")

    def _create_schema(self):
        return Schema(
            message_id=ID(stored=True, unique=True),
            subject=TEXT(stored=True),
            body_plain=TEXT(stored=True),
            attachment_text=TEXT(stored=True),
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
        doc_ids_for_chroma = []
        
        try:
            print("Whoosh 색인을 생성하는 중...")
            for email_obj in parse_eml_files(self.eml_dir):
                receivers_str = ",".join(email_obj.receivers) if email_obj.receivers else ""
                
                writer.add_document(
                    message_id=email_obj.message_id,
                    subject=email_obj.subject if email_obj.subject else "",
                    body_plain=email_obj.body_plain if email_obj.body_plain else "",
                    attachment_text=email_obj.attachment_text if email_obj.attachment_text else "",
                    sender=email_obj.sender if email_obj.sender else "",
                    receivers=receivers_str,
                    sent_date=email_obj.sent_date,
                    folder_path=email_obj.folder_path if email_obj.folder_path else "",
                    thread_topic=email_obj.thread_topic if email_obj.thread_topic else ""
                )
                
                # 시맨틱 검색을 위한 텍스트와 ID 저장 (Whoosh와 ChromaDB 동시 사용)
                text_content = (
                    f"{email_obj.subject if email_obj.subject else ''}\n"
                    f"{email_obj.body_plain if email_obj.body_plain else ''}\n"
                    f"{email_obj.attachment_text if email_obj.attachment_text else ''}"
                )
                texts_to_embed.append(text_content)
                doc_ids_for_chroma.append(email_obj.message_id) # ChromaDB용 ID는 문자열이어야 함
                
                email_count += 1

            writer.commit()
            print(f"{email_count}개의 이메일이 Whoosh 색인에 성공적으로 추가되었습니다.")
        except Exception as e:
            print(f"이메일 색인 중 오류가 발생했습니다: {e}")
            writer.cancel()
            return

        # 2. 시맨틱 검색을 위한 임베딩 생성 및 ChromaDB 저장
        if self.chroma_collection is not None:
            try:
                print("\n시맨틱 검색을 위한 임베딩 벡터를 생성하고 ChromaDB에 저장합니다...")
                print("이 작업은 모델 다운로드를 포함하여 몇 분 정도 소요될 수 있습니다.")
                model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
                
                # 기존 ChromaDB 컬렉션 비우기 (새로운 색인을 위해)
                if self.chroma_collection.count() > 0:
                    self.chroma_client.delete_collection(name="email_embeddings")
                    self.chroma_collection = self.chroma_client.create_collection(name="email_embeddings")
                    print("기존 ChromaDB 컬렉션을 삭제하고 새로 생성했습니다.")

                # ChromaDB는 한번에 많은 문서를 추가할 때 Batch 처리하는 것이 효율적
                BATCH_SIZE = 100
                for i in range(0, len(texts_to_embed), BATCH_SIZE):
                    batch_texts = texts_to_embed[i:i+BATCH_SIZE]
                    batch_ids = doc_ids_for_chroma[i:i+BATCH_SIZE]
                    
                    batch_embeddings = model.encode(batch_texts, show_progress_bar=False).tolist() # ChromaDB는 리스트 형태를 선호
                    
                    self.chroma_collection.add(
                        embeddings=batch_embeddings,
                        documents=batch_texts, # 원본 텍스트도 저장 (선택 사항이지만 유용)
                        ids=batch_ids
                    )
                    print(f"ChromaDB에 {i+len(batch_texts)}개 문서 추가 완료.")
                
                print(f"총 {self.chroma_collection.count()}개의 임베딩이 ChromaDB에 저장되었습니다.")
                
            except Exception as e:
                print(f"임베딩 생성 또는 ChromaDB 저장 중 오류가 발생했습니다: {e}")
        else:
            print("ChromaDB 컬렉션이 초기화되지 않아 임베딩을 저장할 수 없습니다.")


if __name__ == '__main__':
    print("===== Whoosh 검색 색인 및 시맨틱 임베딩 (ChromaDB) 구축 시작 =====")
    indexer = EmailIndexer()
    indexer.index_emails()
    print("===== 모든 색인 작업 완료 =====")