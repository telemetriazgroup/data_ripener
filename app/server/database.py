import os
from pymongo import MongoClient
from dotenv import load_dotenv

load_dotenv()

GENIAL = os.getenv("MONGO_DETAILS_OK")
mongo_db_name = os.getenv("BD_DETAILS_OK")
client = MongoClient(GENIAL)
database_mongo = client[mongo_db_name]

print("*************")
print(GENIAL)
print("*************")
print(mongo_db_name)
print("*************")

def collection(data):
    data_collection = database_mongo.get_collection(data)
    return data_collection



