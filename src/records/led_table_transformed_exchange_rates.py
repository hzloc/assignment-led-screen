import faust

class LedTable(faust.Record, serializer='json'):
    ccy_couple: str
    rate: float
    change: str
