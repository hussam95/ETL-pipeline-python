import os
from re import L
import pandas as pd
from pprint import pprint
from config import CONN_STRING
from pymongo import MongoClient
# Let's connect to MongoDB make a new DB called 'kpis' which will contain a 'collection' per department

client = MongoClient(CONN_STRING)
db=client.kpis 

class Data:

    def __init__(self, file_name, sheet_name):
        self.file_name = file_name
        self.sheet_name = sheet_name
        self.data = pd.read_excel(f"{os.getcwd()}\\{self.file_name}", sheet_name=self.sheet_name)
        
    def DataDictionary(self):
        return self.data.to_dict(orient="records") 

admin = Data("kpis_data_file.xlsx", "admin")
audit = Data("kpis_data_file.xlsx", "audit")
#Step 3: Insert items in admin data dictionary directly into MongoDB via insert_one

for record in admin.DataDictionary():
    result=db.admin.insert_one(record)
    #Step 4: Print to the console the ObjectID of the new document
    #print(result.inserted_id)
print(f"done inserting {len(admin.DataDictionary())} documents in admin collection of kpis db")

for record in audit.DataDictionary():
    result = db.audit.insert_one(record)
print(f"done inserting {len(audit.DataDictionary())} documents in audit collection of kpis db")

pprint(db.audit.find_one({"Month":"Janurary"}))