from pprint import pprint
from pymongo.errors import CollectionInvalid
from DbConnector import DbConnector

class ExampleProgram:
    def __init__(self):
        self.connection = DbConnector()
        self.client = self.connection.client
        self.db = self.connection.db

    def create_coll(self, collection_name: str):
        # Crear solo si no existe (idempotente)
        if collection_name in self.db.list_collection_names():
            print(f"Collection '{collection_name}' already exists, skipping create.")
            return
        try:
            collection = self.db.create_collection(collection_name)
            print('Created collection:', collection)
        except CollectionInvalid:
            print(f"Collection '{collection_name}' already exists (CollectionInvalid).")

    def insert_documents(self, collection_name: str):
        docs = [
            {
                "_id": 1,
                "name": "Bobby",
                "courses": [
                    {'code': 'TDT4225', 'name': 'Very Large, Distributed Data Volumes'},
                    {'code': 'BOI1001', 'name': 'How to become a boi or boierinnaa'}
                ]
            },
            {
                "_id": 2,
                "name": "Bobby",
                "courses": [
                    {'code': 'TDT02', 'name': 'Advanced, Distributed Systems'}
                ]
            },
            {"_id": 3, "name": "Bobby"}
        ]
        collection = self.db[collection_name]
        collection.insert_many(docs)
        print(f"Inserted {len(docs)} docs into '{collection_name}'")

    def fetch_documents(self, collection_name: str):
        collection = self.db[collection_name]
        documents = collection.find({})
        for doc in documents:
            pprint(doc)

    def drop_coll(self, collection_name: str):
        self.db.drop_collection(collection_name)
        print(f"Dropped collection '{collection_name}'")

    def show_coll(self):
        # <-- aquí estaba el typo: self.dbs -> self.db
        collections = self.db.list_collection_names()
        print("Collections:", collections)

def main():
    program = None
    try:
        program = ExampleProgram()
        program.create_coll(collection_name="Person")
        program.show_coll()
        program.insert_documents(collection_name="Person")
        program.fetch_documents(collection_name="Person")
        program.drop_coll(collection_name="Person")
        program.show_coll()
    except Exception as e:
        print("ERROR: Failed to use database:", e)
    finally:
        if program:
            program.connection.close_connection()

if __name__ == '__main__':
    main()
