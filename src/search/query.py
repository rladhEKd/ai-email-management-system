import sys
import os
import re
import numpy as np
import pickle
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from collections import Counter
from src.ingestion.parser import parse_eml_files
import operator
from sentence_transformers import SentenceTransformer
import chromadb # chromadb 임포트

# --- Helper function to analyze contacts from .eml files ---
def get_important_contacts(eml_directory):
    """
    Analyzes the EML email data to identify the main user and their most frequent contacts.
    Returns the main user (email string) and a set of important contacts (email strings).
    """
    print(".eml 파일에서 연락처 분석을 시작합니다...")
    emails = list(parse_eml_files(eml_directory))
    if not emails:
        print("분석할 이메일이 없습니다.")
        return None, set()

    sender_counts = Counter(email.sender for email in emails if email.sender)
    
    sorted_senders = sorted(sender_counts.items(), key=operator.itemgetter(1), reverse=True)
    main_user = None
    for sender, count in sorted_senders:
        if '@' in sender:
            main_user = sender
            break
    if not main_user and sorted_senders:
        main_user = sorted_senders[0][0]

    if not main_user:
        print("메인 사용자(발신자)를 찾을 수 없습니다.")
        return None, set()

    contact_interaction_counts = Counter()
    for email in emails:
        if email.sender == main_user:
            for receiver in email.receivers:
                contact_interaction_counts[receiver] += 1
        elif main_user in email.receivers:
            contact_interaction_counts[email.sender] += 1
    
    important_contacts = {contact for contact, _ in contact_interaction_counts.most_common(10)}

    print(f"메인 사용자(추정): {main_user}")
    print(f"중요 연락처(추정): {important_contacts}")
    
    return main_user, important_contacts
# -----------------------------------------------------------------------------

class Searcher:
    def __init__(self, index_dir="data/index", main_user=None, important_contacts=None, chroma_dir="data/chroma"):
        self.index_dir = index_dir
        self.main_user = main_user
        self.important_contacts = important_contacts if important_contacts is not None else set()
        self.chroma_dir = chroma_dir # ChromaDB 경로 추가
        
        self.ix = None
        self.semantic_model = None
        self.chroma_client = None
        self.chroma_collection = None

        self._open_index()
        self._load_semantic_data()

    def _open_index(self):
        if not os.path.exists(self.index_dir):
            print("오류: Whoosh 색인 디렉토리를 찾을 수 없습니다.")
            return
        try:
            self.ix = open_dir(self.index_dir)
            print("Whoosh 검색 색인을 성공적으로 열었습니다.")
        except Exception as e:
            print(f"Whoosh 색인 파일을 여는 중 오류가 발생했습니다: {e}")
            
    def _load_semantic_data(self):
        if not os.path.exists(self.chroma_dir): # ChromaDB 디렉토리 존재 확인
            print("오류: 시맨틱 검색 데이터(ChromaDB)를 찾을 수 없습니다.")
            print("먼저 'bash -c \"source venv/bin/activate && export PYTHONPATH=$PWD && python3 src/search/indexer.py\"'를 실행하여 색인을 생성해주세요.")
            return
        
        try:
            print("시맨틱 검색 모델과 ChromaDB를 로드합니다...")
            self.semantic_model = SentenceTransformer('paraphrase-multilingual-MiniLM-L12-v2')
            self.chroma_client = chromadb.PersistentClient(path=self.chroma_dir)
            self.chroma_collection = self.chroma_client.get_collection(name="email_embeddings")
            print("시맨틱 데이터 로드를 완료했습니다. ChromaDB 문서 수:", self.chroma_collection.count())
        except Exception as e:
            print(f"시맨틱 데이터를 로드하는 중 오류가 발생했습니다: {e}")

    def _calculate_importance_score(self, email_fields):
        score = 0
        sender = email_fields.get('sender')
        receivers_str = email_fields.get('receivers', '')

        if self.main_user and sender == self.main_user:
            score += 50
        if self.main_user and self.main_user in receivers_str:
            score += 20
        if sender in self.important_contacts:
            score += 30
        return score

    def search(self, query_string, search_fields=["subject", "body_plain", "attachment_text", "sender"], limit=10, semantic_weight=0.5): # search_fields에 attachment_text 추가
        if not self.ix or not self.semantic_model or not self.chroma_collection:
            print("검색기가 준비되지 않았습니다. 색인 및 시맨틱 데이터가 올바르게 로드되었는지 확인하세요.")
            return []

        # --- 1. 독립적인 검색 수행 ---
        all_candidate_scores = {} # message_id -> {'keyword_score': score, 'semantic_score': score}
        
        # 1a. Keyword search (Whoosh)
        max_kw_score = 0.0
        print("키워드 검색 (Whoosh)을 수행합니다...")
        try:
            parser = MultifieldParser(search_fields, schema=self.ix.schema)
            query = parser.parse(query_string)
            with self.ix.searcher() as searcher:
                results = searcher.search(query, limit=None) # 모든 결과 가져오기
                if results:
                    max_kw_score = results[0].score
                    for hit in results:
                        doc_id = hit['message_id']
                        all_candidate_scores[doc_id] = {'keyword_score': hit.score, 'semantic_score': 0.0}
        except Exception as e:
            print(f"키워드 검색 중 오류 발생: {e}")

        # 1b. Semantic search (ChromaDB)
        print("시맨틱 검색 (ChromaDB)을 수행합니다...")
        if self.chroma_collection.count() > 0:
            query_embedding = self.semantic_model.encode([query_string]).tolist()
            
            # ChromaDB에서 시맨틱 검색 수행 (상위 50개 정도 가져옴)
            chroma_results = self.chroma_collection.query(
                query_embeddings=query_embedding,
                n_results=min(50, self.chroma_collection.count()),
                include=['distances'] # IDs와 distances만 필요
            )
            
            if chroma_results and chroma_results['ids']:
                # ChromaDB의 distance는 L2 distance. 0에 가까울수록 유사.
                # 유사도 점수로 변환 (1 / (1 + distance))
                semantic_similarities = {
                    chroma_results['ids'][0][i]: 1 / (1 + chroma_results['distances'][0][i])
                    for i in range(len(chroma_results['ids'][0]))
                }
                
                max_sem_similarity = max(semantic_similarities.values()) if semantic_similarities else 0.0
                
                for doc_id, sim_score in semantic_similarities.items():
                    if doc_id not in all_candidate_scores:
                        all_candidate_scores[doc_id] = {'keyword_score': 0.0, 'semantic_score': 0.0}
                    all_candidate_scores[doc_id]['semantic_score'] = sim_score / max_sem_similarity if max_sem_similarity > 0 else 0


        # --- 2. 결과 병합 및 최종 점수 계산 ---
        combined_results_list = []
        with self.ix.searcher() as s:
            for doc_id, scores in all_candidate_scores.items():
                fields = s.document(message_id=doc_id)
                if not fields:
                    continue
                
                normalized_kw_score = scores['keyword_score'] / max_kw_score if max_kw_score > 0 else 0
                normalized_sem_score = scores['semantic_score'] # 이미 정규화된 것으로 간주

                importance_score = self._calculate_importance_score(fields)
                
                hybrid_score = (1 - semantic_weight) * normalized_kw_score + semantic_weight * normalized_sem_score
                final_score = hybrid_score + (importance_score / 100.0) # 중요도 점수를 보너스로 추가
                
                fields['keyword_score'] = normalized_kw_score
                fields['semantic_score'] = normalized_sem_score
                fields['hybrid_score'] = hybrid_score
                fields['final_score'] = final_score
                combined_results_list.append(fields)

        # --- 3. 최종 결과 정렬 및 반환 ---
        combined_results_list.sort(key=lambda x: x['final_score'], reverse=True)
        return combined_results_list[:limit]

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python3 src/search/query.py \"<검색어>\" [시맨틱 가중치 (0.0-1.0, 기본값: 0.5)]")
        sys.exit(1)

    query_text = sys.argv[1]
    weight = float(sys.argv[2]) if len(sys.argv) > 2 else 0.5
    eml_dir = "eml_output"

    main_user, important_contacts = get_important_contacts(eml_dir)

    if not main_user:
        print("메인 사용자 분석에 실패했습니다. 검색을 시작할 수 없습니다.")
        sys.exit(1)

    print(f"===== '{query_text}' 하이브리드 검색 시작 (시맨틱 가중치: {weight}) =====")
    searcher = Searcher(main_user=main_user, important_contacts=important_contacts)
    search_results = searcher.search(query_text, semantic_weight=weight)

    print("\n--- 검색 결과 (최종 점수 순) ---")
    if not search_results:
        print("결과가 없습니다.")
    else:
        for i, result in enumerate(search_results):
            print(f"\n[결과 {i+1}] (최종 점수: {result.get('final_score', 'N/A'):.4f})")
            print(f"  - 하이브리드 점수: {result.get('hybrid_score', 'N/A'):.4f} (키워드: {result.get('keyword_score', 'N/A'):.4f}, 시맨틱: {result.get('semantic_score', 'N/A'):.4f})")
            print(f"  - 중요도 점수: {searcher._calculate_importance_score(result)}")
            print(f"  - 제목: {result.get('subject')}")
            print(f"  - 발신자: {result.get('sender')}")
            body_snippet = result.get('body_plain', '')[:100].replace('\n', ' ') + "..."
            print(f"  - 본문: {body_snippet}")

    print("\n===== 검색 종료 =====")