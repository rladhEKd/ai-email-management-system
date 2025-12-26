import argparse
import os
from src.ingestion.parser import parse_pst_file
from src.ingestion.storage import SQLiteStorage

def main(pst_file_path, db_path, batch_size=100):
    """
    PST 파일에서 이메일을 파싱하여 SQLite 데이터베이스에 저장하는 메인 함수.
    """
    print("===== 데이터 수집 파이프라인 시작 =====")

    # 1. 스토리지 준비
    # 'data' 폴더가 없으면 생성
    if not os.path.exists(os.path.dirname(db_path)):
        os.makedirs(os.path.dirname(db_path))
        print(f"'{os.path.dirname(db_path)}' 디렉토리 생성 완료.")

    # 기존 DB 파일 삭제 (새로운 수집을 위해)
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
            if len(batch) >= batch_size:
                storage.insert_emails(batch)
                total_inserted += len(batch)
                print(f"{len(batch)}개 이메일 삽입 완료 (총 {total_inserted}개).")
                batch = []

        # 마지막 남은 배치 삽입
        if batch:
            storage.insert_emails(batch)
            total_inserted += len(batch)
            print(f"{len(batch)}개 이메일 삽입 완료 (총 {total_inserted}개).")
            batch = []
            
        print(f"\n총 {total_inserted}개의 이메일이 데이터베이스에 성공적으로 저장되었습니다.")

    except Exception as e:
        print(f"파이프라인 실행 중 오류가 발생했습니다: {e}")
    finally:
        # 3. 스토리지 연결 종료
        storage.close()
        print("\n===== 데이터 수집 파이프라인 종료 =====")


if __name__ == '__main__':
    # 명령줄 인자 파싱 설정
    parser = argparse.ArgumentParser(description="PST 파일에서 이메일을 추출하여 SQLite DB에 저장합니다.")
    parser.add_argument("pst_file", help="파싱할 PST 파일의 경로")
    parser.add_argument("--db-path", default="data/emails.db", help="저장할 SQLite 데이터베이스 파일 경로 (기본값: data/emails.db)")
    parser.add_argument("--batch-size", type=int, default=100, help="한 번에 DB에 삽입할 이메일 수 (기본값: 100)")

    args = parser.parse_args()

    # 메인 함수 실행
    main(args.pst_file, args.db_path, args.batch_size)
