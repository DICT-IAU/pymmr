import sqlalchemy
from sqlalchemy import String, Integer, Text
import pandas as pd
import numpy as np

database_username = 'root'
database_password = 'root'
database_ip       = 'localhost'
database_name     = 'mmr'
database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

df = pd.read_csv("dataset/all_viruses_clean.csv")#, keep_default_na=False)
df.drop(df.columns[[0]], axis=1, inplace=True)
df["collection_date"] = df["collection_date"].str[:4]
df.fillna("N/A", inplace=True)
print(df.head(5))

types = {"id": Integer(), 'gene_symbol': String(255), 'gene_product_name': String(255), 'genbank_genome_accession': String(255), 
    'genbank_protein_accession': String(255), 'strain_name': String(255), 'protein': String(255), 'collection_date': String(4), 
    'host': String(255), 'country': String(255), 'cds': String(255), 'virus_specimen': String(255), 'sequence_type': String(255),
    'sequence': Text()}

df.to_sql(con=database_connection, name='virus', index_label='id', chunksize=10, if_exists='replace', dtype=types)

# Add unique keys:
with database_connection.connect() as con:
    con.execute('ALTER TABLE `virus` ADD PRIMARY KEY (`id`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`genbank_genome_accession`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`genbank_protein_accession`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`virus_specimen`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`sequence_type`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`gene_symbol`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`strain_name`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`collection_date`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`host`);')
    con.execute('ALTER TABLE `virus` ADD INDEX (`country`);')
