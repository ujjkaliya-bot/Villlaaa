"""
One-time helper: copy your OLD PostgreSQL data into MongoDB.
(Skip this if you are starting fresh.)

    pip install asyncpg            # only needed for this script
    export DATABASE_URL="postgres://user:pass@host/db"   # old PostgreSQL
    export MONGO_URI="mongodb+srv://..."                 # new MongoDB
    export MONGO_DB_NAME="otpbot"
    python3 migrate_postgres_to_mongo.py

Safe to re-run: rows are upserted by primary key, nothing is deleted.
NOTE: written without access to a live PostgreSQL/MongoDB - test on a copy first.
"""
import asyncio
import os

import asyncpg
from motor.motor_asyncio import AsyncIOMotorClient

from mongo_compat import TABLES, ensure_schema


async def main():
    pg = await asyncpg.connect(os.environ["DATABASE_URL"])
    mongo = AsyncIOMotorClient(os.environ["MONGO_URI"])
    db = mongo[os.getenv("MONGO_DB_NAME", "otpbot")]
    await ensure_schema(db)

    for table, meta in TABLES.items():
        try:
            rows = await pg.fetch(f"SELECT * FROM {table}")
        except Exception as exc:
            print(f"- {table}: skipped ({exc})")
            continue
        for r in rows:
            doc = dict(r)
            pk = {k: doc[k] for k in meta["pk"]}
            await db[table].update_one(pk, {"$set": {k: v for k, v in doc.items() if k not in pk}}, upsert=True)
        if meta["serial"] and rows:
            top = max(int(r["id"]) for r in rows)
            await db["_counters"].update_one({"_id": table}, {"$max": {"seq": top}}, upsert=True)
        print(f"✅ {table}: {len(rows)} rows copied")

    await pg.close()
    mongo.close()


asyncio.run(main())
