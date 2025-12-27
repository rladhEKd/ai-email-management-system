import sys
import os
import re
from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser
from collections import Counter
from src.ingestion.parser import parse_eml_files # Import the new EML parser
import operator

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
    
    # Find the top sender, preferring those with email addresses
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

    # Determine important contacts by interactions with the main user
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
    def __init__(self, index_dir="data/index", main_user=None, important_contacts=None):
        self.index_dir = index_dir
        self.ix = None
        self.main_user = main_user
        self.important_contacts = important_contacts if important_contacts is not None else set()
        self._open_index()

    def _open_index(self):
        if not os.path.exists(self.index_dir):
            print("오류: 색인 디렉토리을 찾을 수 없습니다.")
            print("먼저 'bash -c \"source venv/bin/activate && export PYTHONPATH=$PWD && python3 src/search/indexer.py\"'를 실행하여 색인을 생성해주세요.")
            return

        try:
            self.ix = open_dir(self.index_dir)
            print("검색 색인을 성공적으로 열었습니다.")
        except Exception as e:
            print(f"색인 파일을 여는 중 오류가 발생했습니다: {e}")

    def _calculate_importance_score(self, email_fields):
        score = 0
        sender = email_fields.get('sender')
        receivers_str = email_fields.get('receivers', '')

        # High bonus if main user is the sender
        if self.main_user and sender == self.main_user:
            score += 50
        
        # Bonus if main user is a recipient
        if self.main_user and self.main_user in receivers_str:
            score += 20
        
        # Bonus if sender is an important contact
        if sender in self.important_contacts:
            score += 30
            
        return score

    def search(self, query_string, search_fields=["subject", "body_plain", "sender"], limit=10):
        if not self.ix:
            return []

        parser = MultifieldParser(search_fields, schema=self.ix.schema)
        try:
            query = parser.parse(query_string)
        except Exception:
            return []

        scored_results = []
        with self.ix.searcher() as searcher:
            results = searcher.search(query, limit=None)
            for hit in results:
                email_fields = hit.fields()
                importance_score = self._calculate_importance_score(email_fields)
                email_fields['importance_score'] = importance_score
                scored_results.append(email_fields)

        scored_results.sort(key=lambda x: x['importance_score'], reverse=True)
        return scored_results[:limit]

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: bash -c \"source venv/bin/activate && export PYTHONPATH=$PWD && python3 src/search/query.py \\\"<검색어>\\\"\"")
        sys.exit(1)

    eml_dir = "eml_output" # The directory with our .eml files
    query_text = sys.argv[1]

    main_user, important_contacts = get_important_contacts(eml_dir)

    if not main_user:
        print("메인 사용자 분석에 실패했습니다. 검색을 시작할 수 없습니다.")
        sys.exit(1)

    print(f"===== '{query_text}' 검색 시작 (메인 사용자: {main_user}) =====")
    searcher = Searcher(main_user=main_user, important_contacts=important_contacts)
    search_results = searcher.search(query_text)

    print("\n--- 검색 결과 (중요도 순) ---")
    if not search_results:
        print("결과가 없습니다.")
    else:
        for i, result in enumerate(search_results):
            print(f"\n[결과 {i+1}] (점수: {result.get('importance_score', 'N/A')})")
            print(f"  Subject: {result.get('subject')}")
            print(f"  Sender: {result.get('sender')}")
            print(f"  Receivers: {result.get('receivers')}")
            print(f"  Date: {result.get('sent_date')}")
            body_snippet = result.get('body_plain', '')[:150].replace('\n', ' ') + "..."
            print(f"  Body: {body_snippet}")

    print("\n===== 검색 종료 =====")
