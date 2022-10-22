import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file = "E:/ebd_relAug-2022/ebd_relAug-2022.txt"

chunksize = 100_000

csv_stream = pd.read_csv(csv_file, sep='\t', chunksize=chunksize, low_memory=False)

countryParquetDict = dict()
for i, chunk in enumerate(csv_stream):
    print("Chunk", i)
    
    chunk = chunk.astype(str)
    if i == 0:
        #  Guess the schema of the CSV file from the first chunk
         parquet_schema = pa.Table.from_pandas(df=chunk).schema

    groupedChunk = chunk.groupby("COUNTRY")
    for country in groupedChunk.groups:
        parquet_file = "E:/eBirdData/countrySplits/{}.parquet".format(country)
        countryChunk = groupedChunk.get_group(country)
        if country in countryParquetDict.keys():
            parquet_writer = countryParquetDict[country]
            table = pa.Table.from_pandas(countryChunk, schema=parquet_schema)
            parquet_writer.write_table(table)
        else:
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            # Write CSV chunk to the parquet file
            table = pa.Table.from_pandas(countryChunk, schema=parquet_schema)
            parquet_writer.write_table(table)
            countryParquetDict[country] = parquet_writer
         

for country, parquetLink in countryParquetDict.items():
    parquetLink.close()



