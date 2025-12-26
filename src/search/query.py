import sys
import os

# Manually add the project root directory to the Python path
project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..'))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from whoosh.index import open_dir
from whoosh.qparser import MultifieldParser, QueryParser
from whoosh.searching import Searcher as WhooshSearcher

class Searcher:
    def __init__(self, index_dir="data/index"):
        self.index_dir = index_dir
        self.ix = None
        self._open_index()

    def _open_index(self):
        """
        Whoosh 색인 디렉토리를 엽니다.
        """
        if not os.path.exists(self.index_dir):
            print(f"오류: 색인 디렉토리 '{self.index_dir}'를 찾을 수 없습니다.")
            print("먼저 'python3 -m src.search.indexer'를 실행하여 색인을 생성해주세요.")
            return

        try:
            self.ix = open_dir(self.index_dir)
            print("검색 색인을 성공적으로 열었습니다.")
        except Exception as e:
            print(f"색인 파일을 여는 중 오류가 발생했습니다: {e}")

    def search(self, query_string, search_fields=["subject", "body_plain", "sender"], limit=10):
        """
        주어진 쿼리 문자열로 이메일을 검색합니다.

        :param query_string: 검색할 키워드
        :param search_fields: 검색할 필드 목록 (기본값: 제목, 본문, 발신자)
        :param limit: 반환할 최대 개수
        :return: 검색 결과 리스트 (딕셔너리 형태)
        """
        if not self.ix:
            print("오류: 색인이 열려있지 않습니다.")
            return []

        # 여러 필드에서 동시에 검색하기 위해 MultifieldParser를 사용합니다.
        parser = MultifieldParser(search_fields, schema=self.ix.schema)

        try:
            query = parser.parse(query_string)
            print(f"파싱된 쿼리: {query}")
        except Exception as e:
            print(f"쿼리 파싱 중 오류가 발생했습니다: {e}")
            return []

        results_list = []
        with self.ix.searcher() as searcher:
            results = searcher.search(query, limit=limit)
            print(f"'{query_string}'에 대해 {len(results)}개의 결과를 찾았습니다.")

            for hit in results:
                results_list.append(hit.fields())

        return results_list

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("사용법: python3 -m src.search.query \"<검색어>\"")
        sys.exit(1)

    query_text = sys.argv[1]

    print(f"===== '{query_text}' 검색 시작 =====")
    searcher = Searcher()
    search_results = searcher.search(query_text)

    print("\n--- 검색 결과 ---")
    if not search_results:
        print("결과가 없습니다.")
    else:
        for i, result in enumerate(search_results):
            print(f"\n[결과 {i+1}]")
            print(f"  Subject: {result.get('subject')}")
            print(f"  Sender: {result.get('sender')}")
            print(f"  Date: {result.get('sent_date')}")
            body_snippet = result.get('body_plain', '')[:150].replace('\n', ' ') + "..."
            print(f"  Body: {body_snippet}")

    print("\n===== 검색 종료 =====")
