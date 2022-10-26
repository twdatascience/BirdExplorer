import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

parquet_file = "E:/eBirdData/allData.parquet"
csv_file = "E:/ebd_relAug-2022/ebd_relAug-2022.txt"

chunkSize = 100_000

csv_stream = pd.read_csv(csv_file, sep = '\t', chunksize = chunkSize, low_memory = False)

for i, chunk in enumerate(csv_stream):
    print("Chunk", i)
    
    chunk = chunk.astype(str)
    if i == 0:
        #  Guess the schema of the CSV file from the first chunk
        parquet_schema = pa.Table.from_pandas(df=chunk).schema
        parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
        
    chunkTable = pa.Table.from_pandas(chunk, schema=parquet_schema)
     # write to new parquet file   
    parquet_writer.write_table(chunkTable)

parquet_writer.close()

    

