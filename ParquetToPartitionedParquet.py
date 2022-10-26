import pyarrow as pa
import pyarrow.parquet as pq
import pyarrow.dataset as ds

allData = pq.read_table("E:/eBirdData/allData.parquet")

ds.write_dataset(allData, "C:/Users/Meso/Documents/GitHub/BirdExplorer/data/countrySplits", format="parquet",
                 partitioning=ds.partitioning(
                    pa.schema([allData.schema.field("COUNTRY")])
                ))