import os
import sys
from email import policy
from email.parser import BytesParser
from email.utils import parsedate_to_datetime, getaddresses
from datetime import datetime
from src.common.models import Email

def parse_eml_files(eml_directory):
    """
    Walks through a directory, parses all .eml files, and yields Email objects.
    """
    print(f"'{eml_directory}' 디렉터리에서 .eml 파일 파싱을 시작합니다...")
    
    file_count = 0
    for root, _, files in os.walk(eml_directory):
        for filename in files:
            if not filename.endswith(".eml"):
                continue

            file_path = os.path.join(root, filename)
            file_count += 1
            
            with open(file_path, 'rb') as f:
                # Use BytesParser with the default policy
                msg = BytesParser(policy=policy.default).parse(f)

            # Extract headers
            subject = msg.get('subject', 'No Subject')
            sender_tuple = getaddresses([msg.get('from', '')])
            sender = sender_tuple[0][1] if sender_tuple else 'No Sender'

            to_tuple = getaddresses(msg.get_all('to', []))
            cc_tuple = getaddresses(msg.get_all('cc', []))
            
            receivers = [addr for name, addr in to_tuple + cc_tuple]

            date_str = msg.get('date')
            sent_date = None
            if date_str:
                try:
                    sent_date = parsedate_to_datetime(date_str)
                except Exception:
                    sent_date = datetime.now() # Fallback

            # Extract body
            body = ""
            if msg.is_multipart():
                for part in msg.walk():
                    ctype = part.get_content_type()
                    cdispo = str(part.get('Content-Disposition'))
                    if ctype == 'text/plain' and 'attachment' not in cdispo:
                        body = part.get_payload(decode=True).decode('utf-8', errors='ignore')
                        break
            else:
                body = msg.get_payload(decode=True).decode('utf-8', errors='ignore')

            yield Email(
                message_id=filename, # Use filename as a unique ID
                subject=subject,
                body_plain=body,
                body_html=None,
                sender=sender,
                receivers=receivers,
                sent_date=sent_date,
                folder_path=os.path.basename(root),
                thread_topic=subject
            )
    print(f"총 {file_count}개의 .eml 파일을 파싱했습니다.")


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("사용법: python3 -m src.ingestion.parser <eml_디렉터리_경로>")
        sys.exit(1)
    
    eml_dir = sys.argv[1]
    
    print("EML 파싱 테스트 시작...")
    email_generator = parse_eml_files(eml_dir)
    
    if email_generator:
        for i, email_obj in enumerate(email_generator):
            if i >= 5:
                print("\n... (테스트 종료, 더 많은 메시지가 있을 수 있음)")
                break
            print(f"\n--- Email Object {i+1} ---")
            print(email_obj)
        
    print("\nEML 파싱 테스트 종료.")