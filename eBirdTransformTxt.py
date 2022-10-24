import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


csv_file = "E:/ebd_relAug-2022/ebd_relAug-2022.txt"

chunkSize = 1_000_000

csv_stream = pd.read_csv(csv_file, sep = '\t', chunksize = chunkSize, low_memory = False)

countryList = list()
for i, chunk in enumerate(csv_stream):
    print("Chunk", i)
    
    chunk = chunk.astype(str)
    if i == 0:
        #  Guess the schema of the CSV file from the first chunk
         parquet_schema = pa.Table.from_pandas(df=chunk).schema

    groupedChunk = chunk.groupby("COUNTRY")
    for country in groupedChunk.groups:
        parquet_file = "C:/Users/Meso/Documents/GitHub/BirdExplorer/data/countrySplits/{}.parquet".format(country)
        countryChunk = groupedChunk.get_group(country)
        if country in countryList: 
            # load in previous chunk(s) data
            countryDatLoad = pq.read_table(parquet_file)
            # transform chunk into arrow table
            chunkTable = pa.Table.from_pandas(countryChunk, schema=parquet_schema)
            # combine previous chunk data and current chunk data
            combinedCountryDat = pa.concat_tables([countryDatLoad, chunkTable])
            # write to new parquet file
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            parquet_writer.write_table(combinedCountryDat)
            parquet_writer.close()
            
        else:
            # convert pandas df to arrow table
            table = pa.Table.from_pandas(countryChunk, schema=parquet_schema)
            # write chunk to parquet file
            parquet_writer = pq.ParquetWriter(parquet_file, parquet_schema, compression='snappy')
            parquet_writer.write_table(table)
            parquet_writer.close()
            # append countryList so this data will be pulled in later
            countryList.append(country)
    

