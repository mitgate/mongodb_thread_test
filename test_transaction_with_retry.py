from motor.motor_asyncio import AsyncIOMotorClient
import pymongo
from pymongo.errors import OperationFailure
from pymongo.read_concern import ReadConcern
from pymongo.write_concern import WriteConcern

from bson.objectid import ObjectId

import concurrent.futures
import functools
from datetime import datetime

from pymongo import MongoClient
import pprint
import ray

import logging

import time

NUMBERS = list(range(100))
RUNS = 2

uri = "mongodb://192.168.224.5:27017/?directConnection=true&authSource=admin/mydatabase"

client = AsyncIOMotorClient(uri)


logging.basicConfig(
    format="%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
    level=logging.DEBUG,
)


def commit_with_retry(session):
    while True:
        try:
            session.commit_transaction()
            print("Transaction committed!!!!")
            break
        except (OperationFailure) as exc:
            if exc.has_error_label("TransientTransactionError"):
                print("TransientTransactionError, tentando novamente... ")
                continue
            else:
                print("Error during commit ...")
                raise


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

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        with client.start_session() as session:
            for _ in range(runs):
                with session.start_transaction(
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern(w="majority"),
                ):
                    x = None
                    for seq in NUMBERS:
                        # print (seq)
                        x = coll_a.find_one({"sequencia": seq})
                        coll_a.update_one(
                            {"_id": x["_id"]},
                            {"$set": {"tipo": x["tipo"] + ", thread A Alterou"}},
                            upsert=False,
                            session=session,
                        )
            commit_with_retry(session)

            for _ in range(runs):
                with session.start_transaction(
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern(w="majority"),
                ):
                    x = None
                    for seq in NUMBERS:
                        # print (seq)
                        x = coll_b.find_one({"sequencia": seq})
                        coll_b.update_one(
                            {"_id": x["_id"]},
                            {"$set": {"tipo": x["tipo"] + ", thread A Alterou"}},
                            upsert=False,
                            session=session,
                        )
            commit_with_retry(session)

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

    with concurrent.futures.ThreadPoolExecutor(max_workers=16) as executor:
        with client.start_session() as session:
            for _ in range(runs):
                with session.start_transaction(
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern(w="majority"),
                ):
                    x = None
                    for seq in NUMBERS:
                        # print (seq)
                        x = coll_a.find_one({"sequencia": seq})
                        coll_a.update_one(
                            {"_id": x["_id"]},
                            {"$set": {"tipo": x["tipo"] + ", thread B Alterou"}},
                            upsert=False,
                            session=session,
                        )
            commit_with_retry(session)

            for _ in range(runs):
                with session.start_transaction(
                    read_concern=ReadConcern("snapshot"),
                    write_concern=WriteConcern(w="majority"),
                ):
                    x = None
                    for seq in NUMBERS:
                        # print (seq)
                        x = coll_b.find_one({"sequencia": seq})
                        coll_b.update_one(
                            {"_id": x["_id"]},
                            {"$set": {"tipo": x["tipo"] + ", thread B Alterou"}},
                            upsert=False,
                            session=session,
                        )
            commit_with_retry(session)

    end = datetime.now()
    logging.info(f"Thread A concorrente: {end - start}")

    return True


def main():
    global RUNS

    logging.info(f"\nCarregando Bases")

    client = MongoClient(uri)
    db = client.basetest

    collA = db["tabela_a"]
    collA.delete_many({})

    collB = db["tabela_b"]
    collB.delete_many({})

    start = datetime.now()

    # insere dados base nas duas collections

    with client.start_session() as session:
        with session.start_transaction(
            read_concern=ReadConcern("snapshot"),
            write_concern=WriteConcern(w="majority"),
        ):

            try:
                for _ in range(RUNS):
                    insert_values(collA, "base inseriu", NUMBERS)
                end = datetime.now()
            except Exception as e:
                print(e)
        commit_with_retry(session)

    with client.start_session() as session:
        with session.start_transaction(
            read_concern=ReadConcern("snapshot"),
            write_concern=WriteConcern(w="majority"),
        ):

            try:
                for _ in range(RUNS):
                    insert_values(collB, "base inseriu", NUMBERS)
                end = datetime.now()
            except Exception as e:
                print(e)
        commit_with_retry(session)

    logging.info(f"\ntempo da gravação sequencial: {end - start}")

    ray.init()

    # inicia duas threads para alterar dados
    ray.get([func1.remote(RUNS, NUMBERS), func2.remote(RUNS, NUMBERS)])


if __name__ == "__main__":
    main()
