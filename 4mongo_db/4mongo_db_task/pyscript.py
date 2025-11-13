from pymongo import MongoClient
from datetime import datetime, timedelta
import json
import os

client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
collection = db["user_events"]
archive = db["archived_users"]

today = datetime.now()   
thirty_days = timedelta(days=30)
fourteen_days = timedelta(days=14)

month_ago = today - thirty_days
twoweeks_ago = today - fourteen_days


new_users = collection.find(
                {"user_info.registration_date": {"$lt": month_ago}}, 
                {"_id" : 0, "user_id" : 1, "event_time" : 1})


new_users_list = [[doc["user_id"], doc["event_time"]] for doc in new_users]
already_archived = {doc["user_id"] for doc in archive.find({}, {"_id": 0, "user_id": 1})}

archived = []
for user , date in new_users_list:
    if user in already_archived:
        continue

    active = collection.find_one({"user_id" : user, "event_time" : {"$gt" : twoweeks_ago}})
    if not active:
        archive.insert_one({"user_id" : user})
        archived.append(user)
     
name = today.strftime("%Y-%m-%d")

data = {
    "date" : name,
    "archived_users_count" : len(archived),
    "archived_user_ids" : archived
}

dir_path = os.path.dirname(__file__)
filename = os.path.join(dir_path, f"{name}.json")
with open(filename, "w", encoding="utf-8") as f:
    json.dump(data, f, indent = 2, ensure_ascii = False)

print(f"\n====== Archiving completed ({name}) ======")
