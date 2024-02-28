import pymongo

if __name__ == '__main__':
    def random_req():
        mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
        db = mongodb_client["datalake_v3"]
        financing_data = db["financing_data"]
        responses = list(financing_data.find({}, {'_id': 0, 'response': 1}))
        import json

        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(responses, f, ensure_ascii=False, indent=4)
