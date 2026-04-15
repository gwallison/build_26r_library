# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field
from typing import List, Optional

class Sample(BaseModel):
    lab_report_id: Optional[str] = Field(None, description="Main ID of the lab report. NO REASONING.")
    lab_name: Optional[str] = Field(None, description="Name of the laboratory. NO REASONING.")
    client_name: Optional[str] = Field(None, description="Name of the client. NO REASONING.")
    received_date: Optional[str] = Field(None, description="Date received (MM/DD/YY). NO REASONING.")
    client_sample_id: Optional[str] = Field(None, description="Client's sample ID. NO REASONING.")
    lab_sample_id: Optional[str] = Field(None, description="Lab's unique sample ID. NO REASONING.")
    collection_date: Optional[str] = Field(None, description="Collection date/time. NO REASONING.")
    matrix: Optional[str] = Field(None, description="Sample matrix. NO REASONING.")
    sample_notes: Optional[str] = Field(None, description="Verbatim notes from PDF about sample condition ONLY. ABSOLUTELY NO REASONING or internal thoughts.")
    extraction_notes: Optional[str] = Field(None, description="Technical notes about data quality ONLY. ABSOLUTELY NO REASONING.")
    f26r_company_name: Optional[str] = Field(None, description="Form 26R company. NO REASONING.")
    f26r_waste_location: Optional[str] = Field(None, description="Form 26R location. NO REASONING.")
    f26r_waste_code: Optional[str] = Field(None, description="Form 26R waste code. NO REASONING.")
    f26r_date_prepared: Optional[str] = Field(None, description="Form 26R date. NO REASONING.")

class Result(BaseModel):
    lab_sample_id: str = Field(..., description="Must match SAMPLES list. NO REASONING.")
    analyte: str = Field(..., description="Chemical name only. NO REASONING.")
    result: Optional[str] = Field(None, description="The reported value. INCLUDE inequality signs (<, >) if present in the report. NO units, NO alphabetic qualifiers like 'U' or 'J' (put those in qualifier_code). NO REASONING.")
    reporting_limit: Optional[str] = Field(None, description="Numerical RL only. NO REASONING.")
    mdl: Optional[str] = Field(None, description="Numerical MDL only. NO REASONING.")
    units: Optional[str] = Field(None, description="Measurement units only. NO REASONING.")
    qualifier_code: Optional[str] = Field(None, description="Data flags (U, J, etc.) ONLY. Inequality signs (<, >) should stay in the 'result' field. NO REASONING.")
    dilution_factor: Optional[str] = Field(None, description="Numerical value only. NO REASONING.")
    analysis_date: Optional[str] = Field(None, description="Date of analysis. NO REASONING.")
    method: Optional[str] = Field(None, description="Method code only. NO REASONING.")
    pdf_page_number: Optional[str] = Field(None, description="Page number. NO REASONING.")

class Qualifier(BaseModel):
    qualifier_code: str = Field(..., description="The code/flag used in the results table")
    description: str = Field(..., description="The full definition of the qualifier flag")

class LaboratoryExtraction(BaseModel):
    reasoning: str = Field(..., description="Briefly explain your extraction logic and any challenges encountered. Keep data fields clean.")
    samples: List[Sample] = Field(..., description="List of unique samples identified in the report")
    results: List[Result] = Field(..., description="List of all analytical results found")
    qualifiers: List[Qualifier] = Field(..., description="Definitions for any qualifier flags found in the report")
