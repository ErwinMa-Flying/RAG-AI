from dataclasses import dataclass
from typing import Dict, Any

# A simple data class to represent a raw document with text and metadata.
@dataclass
class RawDoc:
    text: str
    metadata: Dict[str, Any]