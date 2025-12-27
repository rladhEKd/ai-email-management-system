from collections import Counter
from src.ingestion.parser import parse_json_file
import operator

def analyze_email_contacts(json_file_path):
    """
    Analyzes the JSON email data to identify the main user and their most frequent contacts.
    """
    print(f"'{json_file_path}' 파일 분석 시작...")

    emails = list(parse_json_file(json_file_path))
    if not emails:
        print("분석할 이메일이 없습니다.")
        return

    # 1. Find the most frequent sender, assume they are the "user"
    sender_counts = Counter(email.sender for email in emails if email.sender)
    if not sender_counts:
        print("발신자 정보를 찾을 수 없습니다.")
        return
        
    # Find the top sender, but handle cases where names are just titles like "PM"
    # Prefer senders with email addresses.
    sorted_senders = sorted(sender_counts.items(), key=operator.itemgetter(1), reverse=True)
    main_user = None
    for sender, count in sorted_senders:
        if '@' in sender:
            main_user = sender
            break
    # If no sender with an email is found, fall back to the absolute top sender
    if not main_user:
        main_user = sorted_senders[0][0]

    print(f"\n분석 완료!")
    print("---------------------------------")
    print(f"가장 빈번한 발신자 (메일함 소유자로 추정): {main_user}")
    print("---------------------------------")

    # 2. Count all contacts (senders and receivers) this user interacts with
    contact_counts = Counter()
    for email in emails:
        # If the user sent it, all receivers are contacts
        if email.sender == main_user:
            for receiver in email.receivers:
                contact_counts[receiver] += 1
        # If the user received it, the sender is a contact
        else:
            if main_user in email.receivers:
                contact_counts[email.sender] += 1
    
    print("\n주요 소통 대상 (상위 10명):")
    if not contact_counts:
        print("소통 기록을 찾을 수 없습니다.")
    else:
        for contact, count in contact_counts.most_common(10):
            print(f"- {contact}: {count}회")
    
    return main_user, contact_counts

if __name__ == "__main__":
    json_path = "shipyard_ultra_complex_100.json"
    analyze_email_contacts(json_path)
