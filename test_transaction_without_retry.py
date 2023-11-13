from motor.motor_asyncio import AsyncIOMotorClient

from bson.objectid import ObjectId

import concurrent.futures
import functools
from datetime import datetime

from pymongo import MongoClient
import pprint
import ray

import logging


NUMBERS = list(range(100))
RUNS = 2

uri = "mongodb://192.168.224.5:27017/?directConnection=true&authSource=admin/mydatabase"

client = AsyncIOMotorClient(uri)


logging.basicConfig(
    filename="/tmp/testedb.log",
    filemode="a",
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


ray.init()


def insert_values(coll, tipo, values):
    docs = [{"sequencia": value, "tipo": tipo} for value in values]
    return coll.insert_many(docs)


async def update(coll):
    filter_ = {"someField": "someValue"}
    data = {"someNewField": "aNewValue"}

    update_ = {
        "$set": data,
        "$currentDate": {"updatedAt": True},
        "$setOnInsert": {"createdAt": datetime.utcnow()},
    }


@ray.remote
def func1(runs, NUMBERS):
    logging.basicConfig(
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    logging.info("Iniciando Thread A")

    client = MongoClient(uri)

    db_a = client.basetest
    coll_a = db_a["tabela_a"]

    db_b = client.basetest
    coll_b = db_b["tabela_b"]

    start = datetime.now()
    with client.start_session() as session:
        with session.start_transaction():
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                for _ in range(runs):
                    try:
                        for seq in NUMBERS:
                            # print (seq)
                            x = coll_a.find_one({"sequencia": seq})
                            coll_a.update_one(
                                {"_id": x["_id"]},
                                {"$set": {"tipo": x["tipo"] + ", thread A Alterou"}},
                                upsert=False, session=session
                            )
                    except Exception as e:
                        logging.info(e)

                for _ in range(runs):
                    try:
                        for seq in NUMBERS:
                            # print (seq)
                            x = coll_b.find_one({"sequencia": seq})
                            coll_a.update_one(
                                {"_id": x["_id"]},
                                {"$set": {"tipo": x["tipo"] + ", thread A Alterou"}},
                                upsert=False, session=session
                            )
                    except Exception as e:
                        logging.info(e)

        session.commit_transaction()


    end = datetime.now()
    logging.info(f"Thread A concorrente: {end - start}")

    return True


@ray.remote
def func2(runs, NUMBERS):
    logging.basicConfig(
        format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
        datefmt="%H:%M:%S",
        level=logging.DEBUG,
    )
    logging.info("Iniciando Thread B")

    client = MongoClient(uri)

    db_a = client.basetest
    coll_a = db_a["tabela_a"]

    db_b = client.basetest
    coll_b = db_b["tabela_b"]

    start = datetime.now()
    with client.start_session() as session:
        with session.start_transaction():
            with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
                for _ in range(runs):
                    try:
                        for seq in NUMBERS:
                            # print (seq)
                            x = coll_a.find_one({"sequencia": seq})
                            coll_a.update_one(
                                {"_id": x["_id"]},
                                {"$set": {"tipo": x["tipo"] + ", thread B Alterou"}},
                                upsert=False, session=session
                            )
                    except Exception as e:
                        logging.info(e)

                for _ in range(runs):
                    try:
                        for seq in NUMBERS:
                            # print (seq)
                            x = coll_b.find_one({"sequencia": seq})
                            coll_a.update_one(
                                {"_id": x["_id"]},
                                {"$set": {"tipo": x["tipo"] + ", thread B Alterou"}},
                                upsert=False, session=session
                            )
                    except Exception as e:
                        logging.info(e)

        session.commit_transaction()


    end = datetime.now()
    logging.info(f"Thread B concorrente: {end - start}")

    return True


def main():
    global RUNS

    client = MongoClient(uri)
    db = client.basetest
    
    with client.start_session() as session:
        with session.start_transaction():
            collA = db["tabela_a"]
            collA.delete_many({})

            collB = db["tabela_b"]
            collB.delete_many({})

            start = datetime.now()

            # insere dados base nas duas collections

            try:
                for _ in range(RUNS):
                    insert_values(collA, "base inseriu", NUMBERS)
                end = datetime.now()
            except Exception as e:
                print(e)

            try:
                for _ in range(RUNS):
                    insert_values(collB, "base inseriu", NUMBERS)
                end = datetime.now()
            except Exception as e:
                print(e)

            logging.info(f"tempo da gravação sequencial: {end - start}")
            
            session.commit_transaction()

            # inicia duas threads para alterar dados
            ray.get([func1.remote(RUNS, NUMBERS), func2.remote(RUNS, NUMBERS)])


if __name__ == "__main__":
    main()
