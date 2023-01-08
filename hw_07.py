from typing import List

import databases
import sqlalchemy
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from sqlalchemy import select, func, desc
from datetime import date, datetime
from dateutil.relativedelta import relativedelta


DB_USER = "postgres"
DB_NAME = "postgres"
DB_PASSWORD = "Admin123!"
DB_HOST = "127.0.0.1"

DATABASE_URL = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:5432/{DB_NAME}"


database = databases.Database(DATABASE_URL)

metadata = sqlalchemy.MetaData()

items = sqlalchemy.Table(
    "items",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("name", sqlalchemy.String),
    sqlalchemy.Column("price", sqlalchemy.Float),
)

stores = sqlalchemy.Table(
    "stores",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("address", sqlalchemy.String),
)

sales = sqlalchemy.Table(
    "sales",
    metadata,
    sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
    sqlalchemy.Column("sale_time", sqlalchemy.DateTime, nullable=False, default=datetime.now),
    sqlalchemy.Column("item_id", sqlalchemy.Integer, sqlalchemy.ForeignKey('items.id')),
    sqlalchemy.Column("store_id", sqlalchemy.Integer, sqlalchemy.ForeignKey('stores.id')),
)


engine = sqlalchemy.create_engine(
    DATABASE_URL, echo=False, connect_args={}
)
metadata.create_all(engine)


class Item(BaseModel):
    id: int
    name: str
    price: float

class ItemTop(BaseModel):
    id: int
    name: str
    sales_amount: int

class Store(BaseModel):
    id: int
    address: str

class StoreTop(BaseModel):
    id: int
    address: str
    income: int

class SaleIn(BaseModel):
    item_id: int
    store_id: int

class SaleOut(BaseModel):
    id: int
    sale_time: datetime
    item_id: int
    store_id: int

class HTTPError(BaseModel):
    detail: str

    class Config:
        schema_extra = {
            "example": {"error": "Некорректные данные"},
        }

app = FastAPI(
    title="Отчёт по продажам"
)


@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()


@app.get("/items/", response_model=List[Item])
async def read_items():
    query = items.select()
    return await database.fetch_all(query)

@app.get("/stores/", response_model=List[Store])
async def read_stores():
    query = stores.select()
    return await database.fetch_all(query)

@app.post("/sales/", response_model=SaleOut, responses={
        200: {"model": SaleOut},
        400: {
            "model": HTTPError,
            "description": "Ответ в случае если пользователь прислал некорректное тело запроса",
        },
    },
)
async def create_sales(sale: SaleIn):
    try:
        query = sales.insert().values(sale_time=datetime.utcnow(), item_id=sale.item_id, store_id=sale.store_id)
        last_record_id = await database.execute(query)
        return {**sale.dict(), "id": last_record_id, "sale_time": last_record_id}
    except Exception:
        raise HTTPException(status_code=400, detail="Некорректные данные")

@app.get("/stores/top/", response_model=List[StoreTop])
async def read_top_stores():
    query = select([stores.c.id,
                    stores.c.address,
                    func.sum(items.c.price).label('income')
                    ]).select_from(sales.join(stores).join(items)
                                    ).where(sales.c.sale_time>=date.today() + relativedelta(months=-1)
                                            ).group_by(stores.c.id).order_by(desc(('income'))).limit(10)
    return await database.fetch_all(query)

@app.get("/items/top/", response_model=List[ItemTop])
async def read_top_items():
    query = select([items.c.id,
                    items.c.name,
                    func.count(sales.c.id).label('sales_amount')
                    ]).select_from(sales.join(items)
                                    ).where(sales.c.sale_time>=date.today() + relativedelta(months=-1)
                                            ).group_by(items.c.id).order_by(desc(('sales_amount'))).limit(10)
    return await database.fetch_all(query)