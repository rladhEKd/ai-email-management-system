from datetime import datetime
from aspose.email import MailMessage, MailAddress
from aspose.email.storage.pst import PersonalStorage, FileFormatVersion, MapiMessage

def create_sample_pst():
    """
    Creates a sample PST file with predefined emails for testing.
    """
    file_path = "test_data.pst"
    
    print(f"'{file_path}' 파일 생성 시작...")

    # Define participants
    user = MailAddress("user@example.com", "Current User")
    manager = MailAddress("manager@example.com", "Manager")
    teammate = MailAddress("teammate@example.com", "Teammate")
    vendor = MailAddress("vendor@example.com", "External Vendor")
    group = MailAddress("team-group@example.com", "Team Group")

    # Scenario 1: Direct email from manager to user
    msg1 = MailMessage()
    msg1.subject = "Project Update"
    msg1.body = "Hi, Please provide an update on the project status."
    msg1.sender = manager
    msg1.to.append(user)
    msg1.date = datetime(2025, 12, 26, 10, 0, 0)

    # Scenario 2: Email from teammate to user, with manager CC'd
    msg2 = MailMessage()
    msg2.subject = "Re: Quick Question"
    msg2.body = "Here is the document you requested. I've CC'd the manager for visibility."
    msg2.sender = teammate
    msg2.to.append(user)
    msg2.cc.append(manager)
    msg2.date = datetime(2025, 12, 26, 11, 30, 0)
    
    # Scenario 3: Group email where user is CC'd
    msg3 = MailMessage()
    msg3.subject = "External Announcement"
    msg3.body = "Please be advised of the upcoming maintenance window."
    msg3.sender = vendor
    msg3.to.append(group)
    msg3.cc.append(user)
    msg3.cc.append(teammate)
    msg3.date = datetime(2025, 12, 27, 9, 0, 0)

    # Create a new PST file and add messages
    with PersonalStorage.create(file_path, FileFormatVersion.UNICODE) as pst:
        inbox_folder = pst.root_folder.add_sub_folder("Inbox")
        inbox_folder.add_message(MapiMessage.from_mail_message(msg1))
        inbox_folder.add_message(MapiMessage.from_mail_message(msg2))
        inbox_folder.add_message(MapiMessage.from_mail_message(msg3))
        
    print(f"'{file_path}' 파일이 성공적으로 생성되었고, 3개의 샘플 이메일이 추가되었습니다.")

if __name__ == "__main__":
    create_sample_pst()
