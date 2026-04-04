# -*- coding: utf-8 -*-
from pydantic import BaseModel, Field
from typing import List, Optional

class Sample(BaseModel):
    lab_report_id: Optional[str] = Field(None, description="The main ID of the laboratory report")
    lab_name: Optional[str] = Field(None, description="Name of the laboratory")
    client_name: Optional[str] = Field(None, description="Name of the client who requested the analysis")
    received_date: Optional[str] = Field(None, description="Date the sample was received by the lab")
    client_sample_id: Optional[str] = Field(None, description="The client's own ID for the sample")
    lab_sample_id: Optional[str] = Field(None, description="The laboratory's unique ID for this sample")
    collection_date: Optional[str] = Field(None, description="Date and time the sample was collected")
    matrix: Optional[str] = Field(None, description="Sample matrix (e.g., Water, Solid, Produced Water)")
    sample_notes: Optional[str] = Field(None, description="Any specific notes or observations about the sample condition")
    extraction_notes: Optional[str] = Field(None, description="Notes regarding the LLM extraction or data quality observations")
    f26r_company_name: Optional[str] = Field(None, description="Company name from Form 26R")
    f26r_waste_location: Optional[str] = Field(None, description="Waste generation location from Form 26R")
    f26r_waste_code: Optional[str] = Field(None, description="Waste code from Form 26R")
    f26r_date_prepared: Optional[str] = Field(None, description="Date Form 26R was prepared")

class Result(BaseModel):
    lab_sample_id: str = Field(..., description="Must match the lab_sample_id from the SAMPLES list")
    analyte: str = Field(..., description="The name of the chemical or parameter analyzed")
    result: Optional[str] = Field(None, description="The numerical or qualitative result value")
    reporting_limit: Optional[str] = Field(None, description="The laboratory reporting limit (RL)")
    mdl: Optional[str] = Field(None, description="Method Detection Limit (MDL)")
    units: Optional[str] = Field(None, description="Measurement units (e.g., ug/L, mg/kg, pCi/L)")
    qualifier_code: Optional[str] = Field(None, description="Data qualifier flags (e.g., U, J, *)")
    dilution_factor: Optional[str] = Field(None, description="The dilution factor applied to the sample")
    analysis_date: Optional[str] = Field(None, description="Date and time the analysis was performed")
    method: Optional[str] = Field(None, description="The analytical method used (e.g., EPA 6010D)")
    pdf_page_number: Optional[str] = Field(None, description="The page number in the PDF where this result was found")

class Qualifier(BaseModel):
    qualifier_code: str = Field(..., description="The code/flag used in the results table")
    description: str = Field(..., description="The full definition of the qualifier flag")

class LaboratoryExtraction(BaseModel):
    samples: List[Sample] = Field(..., description="List of unique samples identified in the report")
    results: List[Result] = Field(..., description="List of all analytical results found")
    qualifiers: List[Qualifier] = Field(..., description="Definitions for any qualifier flags found in the report")
