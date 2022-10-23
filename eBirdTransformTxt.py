import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

csv_file = "E:/ebd_relAug-2022/ebd_relAug-2022.txt"

chunkSize = 100_000

csv_stream = pd.read_csv(csv_file, sep='\t', chunksize=chunkSize, low_memory=False)

countryList = list()
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
        if country in countryList: # countryParquetDict.keys():
            countryDatLoad = pq.read_table(parquet_file).to_pandas()
            countryDat = pd.concat([countryDatLoad, countryChunk])
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            table = pa.Table.from_pandas(countryDat, schema=parquet_schema)
            parquet_writer.write_table(table)
            parquet_writer.close()
            
        else:
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            # Write CSV chunk to the parquet file
            table = pa.Table.from_pandas(countryChunk, schema=parquet_schema)
            parquet_writer.write_table(table)
            # countryParquetDict[country] = parquet_writer
            parquet_writer.close()
            countryList.append(country)
    

