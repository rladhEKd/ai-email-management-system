import json
import os
from email.message import EmailMessage
from datetime import datetime

def generate_emls_from_json(json_path, output_dir):
    """
    Reads email data from a JSON file and writes each email as a separate .eml file.
    """
    print(f"'{json_path}'에서 데이터를 읽어 '.eml' 파일 생성을 시작합니다...")
    
    try:
        with open(json_path, 'r', encoding='utf-8') as f:
            emails_data = json.load(f)
    except FileNotFoundError:
        print(f"오류: JSON 파일 '{json_path}'를 찾을 수 없습니다.")
        return
    except json.JSONDecodeError:
        print(f"오류: '{json_path}' 파일이 올바른 JSON 형식이 아닙니다.")
        return

    count = 0
    for i, email_dict in enumerate(emails_data):
        msg = EmailMessage()
        
        # Set headers
        msg['Subject'] = email_dict.get('subject', 'No Subject')
        msg['From'] = email_dict.get('sender', 'No Sender')
        
        # For simplicity, we'll add all receivers to the 'To' field.
        # A more complex setup could distinguish To/Cc if the JSON provided it.
        receivers = email_dict.get('receiver', [])
        if receivers:
            msg['To'] = ", ".join(receivers)
            
        # Set date
        try:
            date_str = email_dict.get("date")
            # The email library expects a specific date format
            dt = datetime.strptime(date_str, "%Y-%m-%d")
            msg['Date'] = dt.strftime("%a, %d %b %Y %H:%M:%S +0000")
        except (ValueError, TypeError):
            pass # Leave date unset if format is wrong

        # Set body
        msg.set_content(email_dict.get('body', ''))

        # Write to .eml file
        file_name = f"email_{i+1}.eml"
        file_path = os.path.join(output_dir, file_name)
        with open(file_path, 'wb') as f:
            f.write(msg.as_bytes())
        count += 1

    print(f"총 {count}개의 '.eml' 파일을 '{output_dir}'에 성공적으로 생성했습니다.")

if __name__ == "__main__":
    generate_emls_from_json("shipyard_ultra_complex_100.json", "eml_output")
