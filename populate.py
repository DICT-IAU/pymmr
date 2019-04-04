import sqlalchemy
from sqlalchemy import String, Integer, Text
import pandas as pd
import numpy as np
import sys

database_username = 'root'
database_password = 'root'
database_ip       = 'localhost'
database_name     = 'mmr'
database_connection = sqlalchemy.create_engine('mysql+pymysql://{0}:{1}@{2}/{3}'.
                                               format(database_username, database_password, 
                                                      database_ip, database_name))

# Epitopse
epitopes = ["mumps", "measles", "rubella"]

for ep in epitopes:
    df = pd.read_csv("dataset/epitopes/%s.csv" % ep)
    df.drop(df.columns[[0]], axis=1, inplace=True)
    df.columns = map(str.lower, df.columns)
    df.columns = map(lambda s: s.replace(" ", "_"), df.columns)

    # map types to.. strings (who cares)
    types = {}
    for c in df.columns:
        types[c] = String(255)
    
    df.to_sql(con=database_connection, name="%s_epitopes" % ep, index_label='id', chunksize=10, if_exists='replace', dtype=types)

    print(df.head(5))

df = pd.read_csv("dataset/all_viruses.csv")#, keep_default_na=False)
df.drop(df.columns[[0]], axis=1, inplace=True)
df = df[df["gene_product_name"].str.startswith("CHECK_") == False]
print(df.head(5))

types = {"id": Integer(), 'gene_symbol': String(255), 'gene_product_name': String(255), 'genbank_genome_accession': String(255), 
    'genbank_protein_accession': String(255), 'strain_name': String(255), 'protein': String(255), 'collection_date': String(4), 
    'host': String(255), 'country': String(255), 'cds': String(255), 'virus_specimen': String(255), 'sequence_type': String(4),
    'fasta': Text()}

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

