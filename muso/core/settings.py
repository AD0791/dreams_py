from pydantic import (
    BaseSettings,
    Field
)
from typing import Dict, Optional, Any



class Setting(BaseSettings):
    email_comcare:str = Field(...,env="EMCOMCARE")
    password_comcare:str = Field(...,env="PASSCOMCARE")
    xmlns:str = Field(...,env="XMLNSBASEURL")
    limit:int = Field(...,env="LIMIT",)
    
    types:str = Field(...,env="TYPE")
    caselimit:int = Field(...,env="CASELIMIT")
    
    form_base_url:str = Field(...,env="FORMBASEURL")
    case_base_url:str = Field(...,env="CASEBASEURL")
    
    class Config:
        env_prefix=""
        case_sensitive=False
        env_file="../muso.env"
        env_file_encoding="utf-8"



setting = Setting()





