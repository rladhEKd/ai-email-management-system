import argparse
import os
import sys
from src.ingestion.parser import parse_pst_file
from src.ingestion.storage import SQLiteStorage
from src.search.indexer import EmailIndexer
from src.search.query import Searcher

def handle_ingest(args):
    """'ingest' 명령어 처리 함수"""
    print("===== 데이터 수집 파이프라인 시작 =====")

    db_path = args.db_path
    pst_file_path = args.pst_file

    if not os.path.exists(pst_file_path):
        print(f"오류: PST 파일 '{pst_file_path}'를 찾을 수 없습니다.")
        return

    # 1. 스토리지 준비
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))
        print(f"'{os.path.dirname(db_path)}' 디렉토리 생성 완료.")

    if os.path.exists(db_path):
        os.remove(db_path)
        print(f"기존 '{db_path}' 파일 삭제 완료.")

    storage = SQLiteStorage(db_path)
    storage.connect()
    if not storage.conn:
        print("오류: 데이터베이스 연결에 실패하여 파이프라인을 중단합니다.")
        return

    storage.create_table()

    # 2. PST 파싱 및 DB 저장
    try:
        email_generator = parse_pst_file(pst_file_path)
        batch = []
        total_inserted = 0

        for email_obj in email_generator:
            batch.append(email_obj)
            if len(batch) >= args.batch_size:
                storage.insert_emails(batch)
                total_inserted += len(batch)
                print(f"{len(batch)}개 이메일 삽입 완료 (총 {total_inserted}개).")
                batch = []

        if batch:
            storage.insert_emails(batch)
            total_inserted += len(batch)
            print(f"{len(batch)}개 이메일 삽입 완료 (총 {total_inserted}개).")

        print(f"\n총 {total_inserted}개의 이메일이 데이터베이스에 성공적으로 저장되었습니다.")
    except Exception as e:
        print(f"파이프라인 실행 중 오류가 발생했습니다: {e}")
    finally:
        storage.close()
        print("\n===== 데이터 수집 파이프라인 종료 =====")

def handle_index(args):
    """'index' 명령어 처리 함수"""
    print("===== Whoosh 검색 색인 구축 시작 =====")
    indexer = EmailIndexer(db_path=args.db_path, index_dir=args.index_dir)
    indexer.index_emails()
    print("===== Whoosh 검색 색인 구축 완료 =====")

def handle_search(args):
    """'search' 명령어 처리 함수"""
    query_text = args.query

    print(f"===== '{query_text}' 검색 시작 =====")
    searcher = Searcher(index_dir=args.index_dir)
    search_results = searcher.search(query_text, limit=args.limit)

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

def main():
    parser = argparse.ArgumentParser(description="PST 이메일 처리 및 검색 시스템")
    subparsers = parser.add_subparsers(dest="command", required=True, help="실행할 명령어")

    # 'ingest' 명령어 파서
    parser_ingest = subparsers.add_parser(
        "ingest", help="PST 파일에서 이메일을 수집하여 DB에 저장합니다."
    )
    parser_ingest.add_argument("pst_file", help="파싱할 PST 파일의 경로")
    parser_ingest.add_argument("--db-path", default="data/emails.db", help="SQLite DB 파일 경로")
    parser_ingest.add_argument("--batch-size", type=int, default=100, help="DB 삽입 배치 크기")
    parser_ingest.set_defaults(func=handle_ingest)

    # 'index' 명령어 파서
    parser_index = subparsers.add_parser(
        "index", help="DB의 이메일을 검색할 수 있도록 색인을 생성합니다."
    )
    parser_index.add_argument("--db-path", default="data/emails.db", help="SQLite DB 파일 경로")
    parser_index.add_argument("--index-dir", default="data/index", help="Whoosh 색인 디렉토리 경로")
    parser_index.set_defaults(func=handle_index)

    # 'search' 명령어 파서
    parser_search = subparsers.add_parser(
        "search", help="색인된 이메일에서 키워드로 검색합니다."
    )
    parser_search.add_argument("query", help="검색할 키워드")
    parser_search.add_argument("--index-dir", default="data/index", help="Whoosh 색인 디렉토리 경로")
    parser_search.add_argument("--limit", type=int, default=10, help="최대 검색 결과 수")
    parser_search.set_defaults(func=handle_search)

    args = parser.parse_args()
    if 'func' in args:
        args.func(args)

if __name__ == '__main__':
    main()
