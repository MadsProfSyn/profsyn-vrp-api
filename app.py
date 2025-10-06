from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List
import os
from dotenv import load_dotenv

# Import your VRP functions
from vrp_scheduler_v2 import run_vrp_for_inspections, save_vrp_results

load_dotenv()

app = FastAPI(title="Profsyn VRP API")

# CORS for your frontend
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Restrict this in production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

class ScheduleRequest(BaseModel):
    inspection_ids: List[str]
    target_dates: List[str]

@app.get("/")
def root():
    return {"status": "Profsyn VRP API Running"}

@app.get("/health")
def health():
    return {"status": "healthy"}

@app.post("/schedule")
def schedule_inspections(request: ScheduleRequest):
    """Run VRP scheduling and return proposed assignments"""
    try:
        result = run_vrp_for_inspections(
            request.inspection_ids,
            request.target_dates
        )
        
        if 'error' in result:
            raise HTTPException(status_code=400, detail=result['error'])
        
        # Save to database
        if 'assignments' in result and len(result['assignments']) > 0:
            run_id = save_vrp_results(result['assignments'], result['metrics'])
            result['run_id'] = run_id
        
        return result
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)