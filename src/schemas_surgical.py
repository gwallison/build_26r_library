# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field
from typing import List, Optional

class SurgicalSample(BaseModel):
    sid: str = Field(..., description="Lab Sample ID")
    cid: Optional[str] = Field(None, description="Client Sample ID")
    rd: Optional[str] = Field(None, description="Received Date")
    cd: Optional[str] = Field(None, description="Collection Date")
    m: Optional[str] = Field(None, description="Matrix")
    bad: bool = Field(False, description="True if scan is poor/blurry")

class SurgicalResult(BaseModel):
    sid: str = Field(..., description="Must match sid in samples")
    a: str = Field(..., description="Analyte name")
    r: Optional[str] = Field(None, description="Value (include < or >)")
    rl: Optional[str] = Field(None, description="Reporting Limit")
    mdl: Optional[str] = Field(None, description="MDL")
    u: Optional[str] = Field(None, description="Units")
    q: Optional[str] = Field(None, description="Qualifiers (U, J, B)")
    p: str = Field(..., description="PDF Page Number (within this file)")

class SurgicalExtraction(BaseModel):
    samples: List[SurgicalSample] = Field(..., description="List of unique samples")
    results: List[SurgicalResult] = Field(..., description="List of all results")

class CompanyExtraction(BaseModel):
    c: Optional[str] = Field(None, description="Client/Ordering Company Name")
    l: Optional[str] = Field(None, description="Analytical Lab Name")
    conf: str = Field(..., description="Confidence: HIGH, LOW, or NONE")
