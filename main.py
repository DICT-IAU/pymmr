from fastapi import FastAPI
from db import Virus, row2dict, result2dict
from sqlalchemy import and_, or_
from typing import List

app = FastAPI()

def getUniqeValues(field_name, filter_):
    col = getattr(Virus, field_name)
    result = Virus.query.with_entities(col).filter(filter_).distinct().all()
    buff = [i[0] for i in result]
    buff = list(sorted(buff, key=lambda x: (x is None, x)))
    return buff

@app.get("/")
def read_root():
    return row2dict(Virus.query.first())

@app.get("/viruses/search/by_accession/{accession_number}")
def read_virus(accession_number: str):
    result = Virus.query.filter(or_(Virus.genbank_genome_accession == accession_number, 
        Virus.genbank_protein_accession == accession_number)).all()
    return result2dict(result)


@app.post("/viruses/search/by_criteria/{virus_specimen}")
def read_virus_by_criteria(virus_specimen: str, gene_symbol: List[str] = None, protein: List[str] = None, host: List[str] = None, 
        country: List[str] = None, collection_date: List[str] = None):
    
    in_gene_symbol = True if gene_symbol is None else Virus.gene_symbol.in_(gene_symbol)
    in_protein = True if protein is None else Virus.protein.in_(protein)
    in_host = True if host is None else Virus.host.in_(host)
    in_country = True if country is None else Virus.country.in_(country)
    in_collection_date = True if collection_date is None else Virus.collection_date.in_(collection_date)

    and_filter = and_(in_gene_symbol, in_protein, in_host, in_country, in_collection_date)
    
    result = Virus.query.filter(and_filter)
    print(result)
    ret = result2dict(result.all())
    print(ret)
    return ret

@app.get("/viruses/search_criteria/{virus_specimen}")
def read_search_criteria(virus_specimen: str, gene_symbol: str = None, protein: str = None, host: str = None, 
        country: str = None, collection_date: int = None):
    print (gene_symbol, protein, host, country, collection_date)

    ftr = Virus.virus_specimen == virus_specimen

    if gene_symbol != None:
        ftr = and_(ftr, Virus.gene_symbol == gene_symbol)
    
    if protein != None:
        ftr = and_(ftr, Virus.protein == protein)
    
    if host != None:
        ftr = and_(ftr, Virus.host == host)

    if country != None:
        ftr = and_(ftr, Virus.country == country)
    
    if collection_date != None:
        ftr = and_(ftr, Virus.collection_date == collection_date)

    ret = {}
    ret["gene_symbol"] = getUniqeValues("gene_symbol", ftr)
    ret["protein"] = getUniqeValues("protein", ftr)
    ret["host"] = getUniqeValues("host", ftr)
    ret["country"] = getUniqeValues("country", ftr)
    ret["collection_date"] = getUniqeValues("collection_date", ftr)

    return ret