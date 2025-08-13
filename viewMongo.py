import sys
import json
try:
    from pymongo import MongoClient
except ImportError:
    print("Install pymongo: pip install pymongo")
    sys.exit(1)

MONGO_URI = "mongodb://localhost:27017/"
MONGO_DB = "weather_traffic_analytics"





def show_table(table_name, limit=None):
    """Show entire table/collection"""
    client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
    db = client[MONGO_DB]
    
    # Check if collection exists
    if table_name not in db.list_collection_names():
        print(f"Table '{table_name}' not found.")
        print(f"Available collections: {', '.join(db.list_collection_names())}")
        client.close()
        return
    
    collection = db[table_name]
    
    # Get total count
    total_docs = collection.count_documents({})
    print(f"Table: {table_name}")
    print(f"Total documents: {total_docs:,}")
    print("="*80)
    
    if total_docs == 0:
        print("No documents found in this collection.")
        client.close()
        return
    
    # Set up query with optional limit
    query = collection.find({})
    if limit:
        query = query.limit(limit)
        print(f"Showing first {limit} documents:\n")
    else:
        print("Showing all documents:\n")
    
    # Print each document
    for i, doc in enumerate(query, 1):
        print(f"Document {i}:")
        print(json.dumps(doc, indent=2, default=str))
        print("-" * 80)
        
        # If showing all docs and there are many, ask user if they want to continue
        if not limit and i % 10 == 0 and i < total_docs:
            try:
                response = input(f"Showed {i}/{total_docs} documents. Continue? (y/n/q): ").lower()
                if response in ['n', 'no', 'q', 'quit']:
                    break
            except KeyboardInterrupt:
                print("\nStopped by user.")
                break
    
    client.close()
def main():
    """Main function"""
    if len(sys.argv) < 3:
        print("Usage: python viewMongo.py table <table_name> [limit]")
        return
    
    table_name = sys.argv[2]
    limit = None
    if len(sys.argv) > 3:
        try:
            limit = int(sys.argv[3])
        except ValueError:
            print("Limit must be a number")
            return
    show_table(table_name, limit)

if __name__ == "__main__":
    main()
