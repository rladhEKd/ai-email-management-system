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
    sent_date: datetime.datetime
    folder_path: str
    attachment_text: Optional[str] = None
    thread_topic: Optional[str] = None
