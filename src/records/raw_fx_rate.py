import faust


class FXRate(faust.Record, serializer='json'):
    event_id: int
    event_time: int
    rate: float
    ccy_couple: str
