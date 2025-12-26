import pypff
import sys
import os
import re
from datetime import datetime
from src.common.models import Email

def _parse_recipients_from_headers(headers_string):
    """
    Parses 'To' and 'Cc' recipients from the transport headers string.
    This is a workaround as pypff doesn't directly expose recipients.
    """
    if not headers_string:
        return []
    
    recipients = []
    # A simple regex to find email addresses
    email_regex = re.compile(r'[\w\.\-]+@[\w\.\-]+')
    
    lines = headers_string.split('\n')
    for line in lines:
        line_lower = line.lower()
        if line_lower.startswith('to:') or line_lower.startswith('cc:'):
            found_emails = email_regex.findall(line)
            recipients.extend(found_emails)
            
    # Remove duplicates and return
    return list(set(recipients))


def message_to_email_object(message, folder_path_str):
    """
    pypff message 객체를 Email 데이터 클래스 객체로 변환합니다.
    """
    sent_date = message.delivery_time if message.delivery_time else None
    plain_body = message.plain_text_body.decode('utf-8', errors='ignore') if message.plain_text_body else None
    html_body = message.html_body.decode('utf-8', errors='ignore') if message.html_body else None
    
    # 수정: transport_headers를 파싱하여 수신자 목록을 가져옵니다.
    receivers = _parse_recipients_from_headers(message.transport_headers)
    
    return Email(
        message_id=message.identifier,
        subject=message.subject,
        body_plain=plain_body,
        body_html=html_body,
        sender=message.sender_name,
        receivers=receivers,
        sent_date=sent_date,
        folder_path=folder_path_str,
        thread_topic=message.conversation_topic
    )

def parse_pst_file(pst_file_path):
    """
    PST 파일을 열고 모든 메시지를 Email 객체로 변환하여 yield합니다.
    """
    if not os.path.exists(pst_file_path):
        print(f"오류: PST 파일 '{pst_file_path}'를 찾을 수 없습니다.")
        return

    pst_file = None
    try:
        pst_file = pypff.file()
        pst_file.open(pst_file_path)
        yield from _traverse_folders(pst_file.get_root_folder())
    except Exception as e:
        print(f"오류: PST 파일을 처리하는 중 문제가 발생했습니다: {e}")
    finally:
        if pst_file:
            pst_file.close()

def _traverse_folders(folder, path_parts=[]):
    """
    재귀적으로 폴더를 순회하며 메시지를 Email 객체로 yield합니다.
    """
    if not folder:
        return
    
    current_path_parts = path_parts + [folder.get_name() or ""]
    folder_path_str = "/".join(current_path_parts)
    
    for message in folder.sub_messages:
        yield message_to_email_object(message, folder_path_str)

    for sub_folder in folder.sub_folders:
        yield from _traverse_folders(sub_folder, current_path_parts)

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python3 -m src.ingestion.parser <PST_파일_경로>")
        sys.exit(1)
    
    pst_path = sys.argv[1]
    
    print("PST 파싱 테스트 시작...")
    email_generator = parse_pst_file(pst_path)
    
    for i, email_obj in enumerate(email_generator):
        if i >= 5:
            print("\n... (테스트 종료, 더 많은 메시지가 있을 수 있음)")
            break
        print(f"\n--- Email Object {i+1} ---")
        print(email_obj)
        
    print("\nPST 파싱 테스트 종료.")