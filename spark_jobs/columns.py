from pyspark.sql import functions as F

from .common import get_spark
from .silver_other import CONFIGS

spark = get_spark("columns")
for name, cfg in CONFIGS.items():
    df = spark.table(f"local.bronze.{name}").limit(500)
    keys = {
        r.k for r in df.select(
            F.explode(F.map_keys(F.from_json("raw_value", "data map<string,string>")["data"])).alias("k")
        ).distinct().collect()
    }
    want = cfg["numeric"] + [cfg["entity_src"], "INTERVENTION"]
    print(f"\n[{name}] MISSING from your data: {[c for c in want if c not in keys]}")
    print(f"[{name}] all columns: {sorted(keys)}")