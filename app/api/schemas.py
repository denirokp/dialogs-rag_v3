from pydantic import BaseModel, Field
from typing import Optional, List

class Mention(BaseModel):
    dialog_id: str
    turn_id: int
    theme: str
    subtheme: Optional[str] = None
    text_quote: str = Field(min_length=1)
    confidence: float = Field(ge=0.0, le=1.0)
    start_char: Optional[int] = None
    end_char: Optional[int] = None

class ExtractionResult(BaseModel):
    mentions: List[Mention]

class QualityReport(BaseModel):
    evidence_100: bool
    client_only_100: bool
    schema_valid_100: bool
    dedup_rate: float
    coverage_other_pct: float
    ambiguity_pct: float
    micro_f1: Optional[float] = None
