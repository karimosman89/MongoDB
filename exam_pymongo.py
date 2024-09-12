from pymongo import MongoClient
from pprint import pprint

# Connect to MongoDB
client = MongoClient(
    host="127.0.0.1",
    port=27017,
    username="admin",
    password="pass",
    authSource="admin",
    authMechanism="SCRAM-SHA-256"
)

# Access the sample database
db = client.sample

#  Display the list of available databases
available_databases = client.list_database_names()
with open('res.txt', 'w') as f:
    f.write(f"Available databases: {available_databases}\n")

# Display the list of collections available in this database
available_collections = db.list_collection_names()
with open('res.txt', 'a') as f:
    f.write(f"Collections in sample database: {available_collections}\n")

#  Display one document from the books collection
one_document = db.books.find_one()
with open('res.txt', 'a') as f:
    f.write(f"One document from books collection:\n")
    pprint(one_document, stream=f)

#  Display the number of documents in the books collection
num_documents = db.books.count_documents({})
with open('res.txt', 'a') as f:
    f.write(f"Number of documents in books collection: {num_documents}\n")

#  Display the number of books with more than 400 pages
count_books_gt_400_pages = db.books.count_documents({"pageCount": {"$gt": 400}})
# Display the number of books with more than 400 pages and published
count_books_gt_400_pages_published = db.books.count_documents({"pageCount": {"$gt": 400}, "status": "PUBLISH"})
with open('res.txt', 'a') as f:
    f.write(f"Number of books with more than 400 pages: {count_books_gt_400_pages}\n")
    f.write(f"Number of books with more than 400 pages and published: {count_books_gt_400_pages_published}\n")

#  Display the number of books with the keyword Android in their description
count_books_android_keyword = db.books.count_documents({
    "$or": [
        {"shortDescription": {"$regex": "Android", "$options": "i"}},
        {"longDescription": {"$regex": "Android", "$options": "i"}}
    ]
})
with open('res.txt', 'a') as f:
    f.write(f"Number of books with keyword 'Android' in description: {count_books_android_keyword}\n")

#  Display the 2 distinct category lists
distinct_categories = db.books.aggregate([
    {"$group": {"_id": None, "categories0": {"$addToSet": {"$arrayElemAt": ["$categories", 0]}},
                "categories1": {"$addToSet": {"$arrayElemAt": ["$categories", 1]}}}}
])
with open('res.txt', 'a') as f:
    f.write("Distinct categories:\n")
    for doc in distinct_categories:
        pprint(doc, stream=f)

# Display the number of books containing specific languages in their long description
count_books_languages = db.books.count_documents({
    "longDescription": {"$regex": "Python|Java|C\+\+|Scala", "$options": "i"}
})
with open('res.txt', 'a') as f:
    f.write(f"Number of books containing Python, Java, C++, or Scala in long description: {count_books_languages}\n")

#  Display statistical information about the database: max, min, avg pages per category
statistical_info = db.books.aggregate([
    {"$unwind": "$categories"},
    {"$group": {"_id": "$categories",
                "maxPages": {"$max": "$pageCount"},
                "minPages": {"$min": "$pageCount"},
                "avgPages": {"$avg": "$pageCount"}}}
])
with open('res.txt', 'a') as f:
    f.write("Statistical information about database:\n")
    for doc in statistical_info:
        pprint(doc, stream=f)

#  Extract year, month, day from published date and filter books published after 2009
extracted_info = db.books.aggregate([
    {"$project": {
        "year": {"$year": "$publishedDate"},
        "month": {"$month": "$publishedDate"},
        "day": {"$dayOfMonth": "$publishedDate"}
    }},
    {"$match": {"year": {"$gt": 2009}}},
    {"$limit": 20}
])
with open('res.txt', 'a') as f:
    f.write("First 20 books published after 2009:\n")
    for doc in extracted_info:
        pprint(doc, stream=f)

# Create new attributes for authors and display first 20 in chronological order
authors_info = db.books.aggregate([
    {"$project": {
        "authors": 1,
        "first_author": {"$arrayElemAt": ["$authors", 0]}
    }},
    {"$sort": {"first_author": 1}},
    {"$limit": 20}
])
with open('res.txt', 'a') as f:
    f.write("First 20 authors in chronological order:\n")
    for doc in authors_info:
        pprint(doc, stream=f)

# Aggregate based on first author and display top 10 most prolific authors
prolific_authors = db.books.aggregate([
    {"$project": {
        "first_author": {"$arrayElemAt": ["$authors", 0]}
    }},
    {"$group": {
        "_id": "$first_author",
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$limit": 10}
])
with open('res.txt', 'a') as f:
    f.write("Top 10 most prolific first authors:\n")
    for doc in prolific_authors:
        pprint(doc, stream=f)

#  Display distribution of number of authors
authors_distribution = db.books.aggregate([
    {"$project": {
        "num_authors": {"$size": "$authors"}
    }},
    {"$group": {
        "_id": "$num_authors",
        "count": {"$sum": 1}
    }},
    {"$sort": {"_id": 1}}
])
with open('res.txt', 'a') as f:
    f.write("Distribution of number of authors:\n")
    for doc in authors_distribution:
        pprint(doc, stream=f)

#  Display occurrence of each author index
authors_occurrence = db.books.aggregate([
    {"$unwind": "$authors"},
    {"$match": {"authors": {"$ne": ""}}},
    {"$group": {
        "_id": "$authors",
        "count": {"$sum": 1}
    }},
    {"$sort": {"count": -1}},
    {"$limit": 20}
])
with open('res.txt', 'a') as f:
    f.write("Occurrence of each author by index:\n")
    for doc in authors_occurrence:
        pprint(doc, stream=f)

client.close()
