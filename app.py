from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from typing import List
import os
from dotenv import load_dotenv
import traceback

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
        print(f"📥 Received request: {len(request.inspection_ids)} inspections, dates: {request.target_dates}")
        
        # Run VRP
        result = run_vrp_for_inspections(
            request.inspection_ids,
            request.target_dates
        )
        
        print(f"✅ VRP completed: {result.get('metrics', {})}")
        
        # Check for VRP errors
        if 'error' in result:
            print(f"❌ VRP returned error: {result['error']}")
            return JSONResponse(
                status_code=400,
                content={"error": result['error']}
            )
        
        # Try to save to database (but don't fail if this crashes)
        if 'assignments' in result and len(result['assignments']) > 0:
            try:
                print(f"💾 Attempting to save {len(result['assignments'])} assignments...")
                run_id = save_vrp_results(result['assignments'], result['metrics'])
                result['run_id'] = run_id
                print(f"✅ Saved with run_id: {run_id}")
            except Exception as save_error:
                print(f"⚠️  Warning: Could not save results to database: {save_error}")
                traceback.print_exc()
                # Don't fail - VRP worked even if saving failed
                result['run_id'] = None
                result['save_warning'] = f"Results not saved: {str(save_error)}"
        
        print(f"✅ Returning successful response")
        return JSONResponse(
            status_code=200,
            content=result
        )
    
    except Exception as e:
        print(f"❌ ERROR in /schedule endpoint: {str(e)}")
        traceback.print_exc()
        return JSONResponse(
            status_code=500,
            content={"error": str(e), "details": traceback.format_exc()}
        )

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
