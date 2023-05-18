from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class Order(BaseModel):
    symbol: str
    action: str
    quantity: int

received_symbol = None  
action = None 
order = None  

@app.post("/api/place_order")
def place_order(order: Order):
    global action

    if order.action == "buy":
        action = "buy"
        return {"message": f"Buy order placed for symbol {order.symbol} with quantity {order.quantity}"}
    elif order.action == "sell":
        action = "sell"
        return {"message": f"Sell order placed for symbol {order.symbol} with quantity {order.quantity}"}
    else:
        raise HTTPException(status_code=400, detail="Invalid action provided")


@app.get("/api/order_book/{symbol}")
def get_order_book(symbol: str):
    global order
   
    order_book = {
        "symbol": symbol,
        "bids": [
            {"price": 100.0, "quantity": 10},
            {"price": 99.5, "quantity": 5},
            {"price": 99.0, "quantity": 8}
        ],
        "asks": [
            {"price": 101.0, "quantity": 15},
            {"price": 101.5, "quantity": 7},
            {"price": 102.0, "quantity": 12}
        ]
    }
    order = order_book
    return order_book


@app.post("/api/received_symbol")
def set_received_symbol(payload: Dict[str, str]):
    global received_symbol
    received_symbol = payload['symbol']
    return {"message": "Received symbol updated successfully"}

order_list1 = [] 
order_list2 = [] 
order_list3 = [] 

@app.get("/api/received_symbol")
def get_received_symbol():
    global received_symbol, action, order
    order_list1.append(received_symbol)
    order_list2.append(action)
    order_list3.append(order)
    return {"symbol": order_list1,"action": order_list2,"order": order_list3}
