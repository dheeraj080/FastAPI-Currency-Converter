import uvicorn
from fastapi import FastAPI
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
import os

load_dotenv()

app = FastAPI()

DATABASE_URL = (os.getenv('SUPABASE_URL'))
#
try:
    engine = create_engine(DATABASE_URL)
    connection = engine.connect()
    print("DB CONNECTED")
    connection.close()
except:
    print("DB FAILED")
    
try:
    with engine.connect() as connection:
        result = connection.execute(text("SELECT * FROM exchange_rates"))
        print("Data from DB")
        for row in result:
            print(row)
            
except Exception as e:
    print("Error",e)



if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
