import logging
import uvicorn
from api import router
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Set up root logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)

app = FastAPI(title="Currency Exchange API")

# Add CORS Middleware for browser access
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Adjust this in strict production to specific domains
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)

if __name__ == "__main__":
    # Removed reload=True for production safety
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
