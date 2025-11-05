"""
Flask API with Job Queue for VRP Scheduler
Replace your app.py with this file
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from dotenv import load_dotenv
import traceback
import uuid
from datetime import datetime
from typing import Optional, Dict, List

# Import VRP functions
from vrp_scheduler_v2 import run_vrp_for_inspections, save_vrp_results, supabase, JobQueue, job_queue

load_dotenv()

app = Flask(__name__)

# CORS for your frontend
CORS(app, origins=["*"])  # Restrict this in production

# ============================================================================
# HEALTH & INFO ENDPOINTS
# ============================================================================

@app.route("/")
def root():
    return jsonify({"status": "Profsyn VRP API with Job Queue", "version": "2.0"})

@app.route("/health")
def health():
    return jsonify({"status": "healthy"})

# ============================================================================
# JOB QUEUE ENDPOINTS
# ============================================================================

@app.route("/api/vrp-available", methods=["GET"])
def vrp_available():
    """Check if VRP is available to accept new jobs"""
    try:
        can_start = job_queue.can_start_job('vrp_calculation')
        
        return jsonify({
            'available': can_start,
            'message': 'VRP scheduler is available' if can_start 
                      else 'VRP scheduler is currently busy'
        }), 200
    
    except Exception as e:
        print(f"Error in /api/vrp-available: {str(e)}")
        return jsonify({
            'available': False,
            'error': f'Server error: {str(e)}'
        }), 500

@app.route("/api/schedule", methods=["POST"])
def schedule_inspections():
    """
    Start a new VRP job with queue management.
    Prevents concurrent executions.
    """
    try:
        data = request.get_json()
        
        # Validate required fields
        if not data or 'inspection_ids' not in data or 'target_dates' not in data:
            return jsonify({
                'error': 'Missing required fields: inspection_ids and target_dates'
            }), 400
        
        inspection_ids = data['inspection_ids']
        target_dates = data['target_dates']
        requested_by = data.get('requested_by', 'api')
        
        # Validate data types
        if not isinstance(inspection_ids, list) or not isinstance(target_dates, list):
            return jsonify({
                'error': 'inspection_ids and target_dates must be arrays'
            }), 400
        
        if len(inspection_ids) == 0:
            return jsonify({
                'error': 'inspection_ids cannot be empty'
            }), 400
        
        print(f"📥 Received request: {len(inspection_ids)} inspections, dates: {target_dates}")
        
        # Check if another job is running
        if not job_queue.can_start_job():
            return jsonify({
                'status': 'blocked',
                'message': 'Another VRP calculation is currently running. Please wait and try again.',
                'retry_after': 30
            }), 409
        
        # Create job
        job_id = job_queue.create_job(
            job_type='vrp_calculation',
            params={
                'inspection_ids': inspection_ids,
                'target_dates': target_dates,
                'requested_by': requested_by
            }
        )
        
        if not job_id:
            return jsonify({
                'error': 'Failed to create job in queue',
                'status': 'error'
            }), 500
        
        print(f"🚀 Starting VRP Job: {job_id}")
        
        # Start the job
        if not job_queue.start_job(job_id):
            return jsonify({
                'error': 'Failed to start job',
                'status': 'error',
                'job_id': job_id
            }), 500
        
        # Run the actual VRP calculation
        result = run_vrp_for_inspections(inspection_ids, target_dates)
        
        # Check if there was an error in VRP execution
        if 'error' in result:
            job_queue.fail_job(job_id, result['error'])
            return jsonify({
                'error': result['error'],
                'status': 'failed',
                'job_id': job_id
            }), 500
        
        # Save results to database
        run_id = save_vrp_results(result['assignments'], result['metrics'])
        
        # Complete the job with results summary
        job_queue.complete_job(job_id, {
            'vrp_run_id': run_id,
            'total_scheduled': result['metrics']['total_scheduled'],
            'total_unscheduled': result['metrics']['total_unscheduled'],
            'total_travel_minutes': result['metrics']['total_travel_minutes'],
            'total_travel_km': result['metrics'].get('total_travel_km', 0),
            'execution_seconds': result['metrics']['execution_seconds']
        })
        
        print(f"✅ VRP Job Completed: {job_id}")
        
        return jsonify({
            'status': 'completed',
            'job_id': job_id,
            'vrp_run_id': run_id,
            'summary': {
                'total_scheduled': result['metrics']['total_scheduled'],
                'total_unscheduled': result['metrics']['total_unscheduled'],
                'total_travel_minutes': result['metrics']['total_travel_minutes'],
                'total_travel_km': result['metrics'].get('total_travel_km', 0),
                'execution_seconds': result['metrics']['execution_seconds']
            }
        }), 200
        
    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"❌ VRP Job Failed")
        print(f"Error: {error_msg}")
        
        if 'job_id' in locals():
            job_queue.fail_job(job_id, error_msg)
        
        return jsonify({
            'error': str(e),
            'status': 'failed',
            'job_id': locals().get('job_id')
        }), 500

@app.route("/api/job-status/<job_id>", methods=["GET"])
def check_job_status(job_id):
    """
    Check the status of a VRP job.
    Used by frontend for polling.
    """
    try:
        status = job_queue.get_job_status(job_id)
        
        if not status:
            return jsonify({
                'error': 'Job not found',
                'job_id': job_id
            }), 404
        
        return jsonify({
            'job_id': job_id,
            'status': status['status'],
            'created_at': status.get('created_at'),
            'started_at': status.get('started_at'),
            'completed_at': status.get('completed_at'),
            'result': status.get('result'),
            'error': status.get('error')
        }), 200
    
    except Exception as e:
        print(f"Error in /api/job-status: {str(e)}")
        return jsonify({
            'error': f'Server error: {str(e)}'
        }), 500

# ============================================================================
# LEGACY ENDPOINT (Keep for backwards compatibility during transition)
# ============================================================================

@app.route("/schedule", methods=["POST"])
def schedule_legacy():
    """
    Legacy endpoint - redirects to new job queue system.
    Remove this after edge function is updated.
    """
    print("⚠️  WARNING: Using legacy /schedule endpoint. Update to /api/schedule")
    return schedule_inspections()

# ============================================================================
# RUN THE APP
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    app.run(host="0.0.0.0", port=port, debug=False)
