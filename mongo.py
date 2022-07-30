from config import CONN_STRING, DB_USERNAME, DB_PASSWORD
from pymongo import MongoClient
# pprint library is used to make the output look more pretty
from pprint import pprint


# # connect to MongoDB using connection string
client = MongoClient(CONN_STRING)
db=client.kpis
pprint(db.admin.find_all())