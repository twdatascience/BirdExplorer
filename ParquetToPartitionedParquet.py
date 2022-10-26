from datetime import datetime

import pyarrow as pa
# import pyarrow.parquet as pq
import pyarrow.dataset as ds
from pyarrow import csv

print(datetime.now.time())

csv_file = "E:/ebd_relAug-2022/ebd_relAug-2022.txt"
parquet_folder = "E:/eBirdData/partCountry"

inputData = csv.open_csv(
    csv_file, 
    parse_options = csv.ParseOptions(
        delimiter = "\t"
        ),
    convert_options = csv.ConvertOptions(
        strings_can_be_null = True,
        column_types = {
            'REASON' : pa.string()
        }
    ))

print("moving to write dataset")
print(datetime.now.time())

ds.write_dataset(inputData, parquet_folder, format = "parquet", partitioning = ds.partitioning(
    pa.schema([inputData.schema.field("COUNTRY")])
))

print("done")
print(datetime.now.time())