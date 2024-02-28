
import pymysql
from prefect import flow, task
import pymongo
import logging


@task()
def extract_finance_data():
    mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
    db = mongodb_client["datalake_v3"]
    order = db["order"]

    docs = list(order.find({"url": "/internal/v2/financing"}, {"_id": 0, "request.body": 1}))
    records = []
    for doc in docs:
        records.append((doc.get('request').get('body').get('order_id'),
                        doc.get('request').get('body').get('transaction_id'),
                        doc.get('request').get('body').get('merchant_id'),
                        doc.get('request').get('body').get('store_id'),
                        doc.get('request').get('body').get('application_id')
                        ))

    con = pymysql.connect(host='localhost', port=3306, user='root', passwd='admin', db='pos_datalake_v2')
    cursor = con.cursor()

    insert = """
                INSERT INTO finance_request 
                (order_id, transaction_id, merchant_id, store_id, application_id)
                VALUES (%s, %s, %s, %s, %s)
            """

    cursor.executemany(insert, records)
    con.commit()

    cursor.close()
    con.close()


#     mongodb_client = pymongo.MongoClient("mongodb://localhost:27017/")
#     db = mongodb_client["datalake_v3"]
#     order = db["order"]
#     financing_data = db["financing_data"]
#     query = {"url": "/internal/v2/financing"}
#     docs = order.find(query)
#     try:
#         if docs:
#             financing_data.drop()
#             financing_data.insert_many(docs)
#             logging.info("finance data refreshed")
#         else:
#             logging.warning("no finance data found for the current run")
#     except Exception:
#         logging.error("Error in Finance data refreshing pipeline")
#         raise Exception


@flow()
def finance_data_flow():
    extract_finance_data()


if __name__ == '__main__':
    finance_data_flow()