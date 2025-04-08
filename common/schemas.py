from pydantic import BaseModel

class TransactionMessage(BaseModel):
    tx_id: int
    amount: float
