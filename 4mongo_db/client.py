from pymongo import MongoClient
from pprint import pprint
import json

# Подключение к MongoDB
client = MongoClient("mongodb://localhost:27017/")
db = client["alcomarket"]
products = db["products"]

# 2. Очистка коллекции перед загрузкой (для повторного запуска)
products.drop()

# 3. Загрузка данных из файла products.json
with open("products.json", "r") as f:
    data = json.load(f)
    products.insert_many(data)

print("\n📦 Все товары:")
for doc in products.find():
    pprint(doc)

print("\n Wine rating.sommelier > 4.5:")
for doc in products.find({"type": "wine", "rating.sommelier": {"$gt": 4.5}}): 
    pprint(doc)

# $eq    ==    equal to
# $ne    !=    not equal to
# $gt    >     greater than
# $gte   >=    greater than or equal to
# $lt    <     less than
# $lte   <=    less than or equal to
# $in           value is in the list
# $nin          value is not in the list
# $and          logical AND
# $or           logical OR
# $not          negation (NOT)
# $exists       field exists
# $regex        matches regular expression pattern

print("\n📑 Name + price")
for doc in products.find({}, {"name": 1, "price": 1, "_id": 0}):  # 1 - for print, 0 - to omit
    pprint(doc)

print("\n🌍 Unique countries:")
pprint(products.distinct("country"))

print("\n📊 AVG price for beer per country")
pipeline = [
    {"$match": {"type": "beer"}},
    {"$group": {"_id": "$country", "avgPrice": {"$avg": "$price"}}},
    {"$sort": {"avgPrice": -1}}
]
for doc in products.aggregate(pipeline):
    pprint(doc)

#aggregation pipeline

# $match == WHERE
# {"$match": {"type": "beer"}}  === SELECT * FROM products WHERE type = 'beer';

# $group == GROUP BY + AVG
# {"$group": {"_id": "$country", "avgPrice": {"$avg": "$price"}}} === SELECT country, AVG("price") AS av_prive FROM products ... GROUP BY country

# $sort == ORDER BY
# {"$sort": {"avgPrice": -1}} == ORDER BY av_price DESC 
# (-1 = DESC, 1 = ASC)


print("\n🔄 Undate price")
products.update_one({"name": "Guinness Draught"}, {"$set": {"price": 2.99}})
pprint(products.find_one({"name": "Guinness Draught"}))

print("\n➕ Add stock")
products.update_many({}, {"$set": {"stock": 100}})
pprint(products.find_one())

# .update_many( {}, {"$set" : { "___" : "___"}})

print("\n❌ Delete unavailable:")
result = products.delete_many({"available": False})  #result --> info 
print(f"Deleted: {result.deleted_count}")

#result.deleted_count --> count of deleted files
#result.acknowledged --> True/False (done)

print("\n📦 ")
for doc in products.find():
    pprint(doc)

print("\n📚 Index info")
pprint(products.index_information())

products.create_index("type")

pprint(products.index_information())