from dataclasses import dataclass, field
from typing import List, Optional
import datetime

@dataclass
class Email:
    message_id: str
    subject: Optional[str]
    body_plain: Optional[str]
    body_html: Optional[str]
    sender: str
    receivers: List[str]
    sent_date: datetime.datetime # 또는 datetime 객체
    folder_path: str
    thread_topic: Optional[str] = None
