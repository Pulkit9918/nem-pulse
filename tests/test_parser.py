from ingest.parser import nem_to_utc_iso, parse_mms_csv

SAMPLE = '''C,NEMP.WORLD,DISPATCHIS,AEMO,PUBLIC,2026/10/11,12:20:05,0000000542309418,DISPATCHIS,0000000542309418
I,DISPATCH,PRICE,5,SETTLEMENTDATE,RUNNO,REGIONID,DISPATCHINTERVAL,INTERVENTION,RRP
D,DISPATCH,PRICE,5,"2026/10/11 12:25:00",1,NSW1,20261011101,0,87.45
D,DISPATCH,PRICE,5,"2026/10/11 12:25:00",1,SA1,20261011101,0,-12.30
C,END OF REPORT,5
'''


def test_parses_price_rows():
    rows = list(parse_mms_csv(SAMPLE))
    assert len(rows) == 2
    table, rec = rows[0]
    assert table == "DISPATCH.PRICE"
    assert rec["REGIONID"] == "NSW1"
    assert float(rec["RRP"]) == 87.45
    assert float(rows[1][1]["RRP"]) == -12.30 


def test_nem_time_is_utc_plus_10():
    assert nem_to_utc_iso("2026/10/11 12:25:00").startswith("2026-10-11T02:25:00")