import os
import re
import pyarrow as pa
import pyarrow.parquet as pq
import pdb

base_path = "E:/eBirdData/partCountry"
countrySplits_path = "C:/Users/Meso/Documents/GitHub/BirdExplorer/data/countrySplits/"

countryNames = dict()

for (dirpath, dirnames, filenames) in os.walk(base_path):
    print(dirpath)
    if dirpath == base_path: continue
    country = re.search('COUNTRY=(.*)', dirpath).group(1)
    countryNames[country] = (dirpath, filenames)

for countryName, pathsTup in countryNames.items():
    print(countryName)
    newFolder = os.path.join(countrySplits_path, countryName)
    if not os.path.exists(newFolder):
        os.makedirs(newFolder)
    parquet_file = os.path.join(newFolder, "{}.parquet".format(countryName))
    pdb.set_trace()
    if os.path.exists(parquet_file):
        continue
    batchTables = list()
    for batchFile in pathsTup[1]:
        batchTables.append(pq.read_table(os.path.join(pathsTup[0], batchFile)))
    print("combining tables")
    combinedTable = pa.concat_tables(batchTables)
    print("writing to dataset")
   
    parquet_writer = pq.ParquetWriter(parquet_file, schema=batchTables[0].schema, compression="snappy")
    parquet_writer.write_table(combinedTable)
    parquet_writer.close()
    del batchTables
    del combinedTable

