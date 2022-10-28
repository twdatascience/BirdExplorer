import pyarrow as pa
import pyarrow.parquet as pq
# import pyarrow.dataset as ds
from pyarrow import csv
import os

csv_file = "E:/ebd_relAug-2022/ebd_relAug-2022.txt"
parquet_folder = "E:/eBirdData/partCountry"

parse_options = csv.ParseOptions(delimiter = "\t")

convert_options = csv.ConvertOptions(
    strings_can_be_null = True,
    column_types = {
        'REASON' : pa.string()
        })

read_options = csv.ReadOptions(block_size = 100000000)


datSchema = None
chunkIndex = 0
with csv.open_csv(
    csv_file, 
    parse_options = parse_options, 
    convert_options = convert_options, 
    read_options = read_options) as reader:
    for chunk in reader:
        print(chunkIndex)
        if chunk is None:
            break
        if datSchema is None:
            datSchema = chunk.schema
        table = pa.Table.from_batches([chunk])
        # chunk_folder = "{}/chunk_{}".format(parquet_folder, str(chunkIndex))
        # os.makedirs(chunk_folder)
        pq.write_to_dataset(table, root_path = parquet_folder,
                    partition_cols = ['COUNTRY'])
        chunkIndex += 1
