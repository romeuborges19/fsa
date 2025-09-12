from pydantic import BaseModel, Field


class RespostaLLM(BaseModel):
    decisao: str = Field(description="A decisão sugerida")
    motivo: str = Field(description="A justificativa para a decisão, em até 64 tokens")
