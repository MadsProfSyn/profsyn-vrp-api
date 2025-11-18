"""
Flask API with Job Queue for VRP Scheduler
Worker-compatible version - creates jobs but doesn't execute them
"""

from flask import Flask, request, jsonify
from flask_cors import CORS
import os
from dotenv import load_dotenv
import traceback
from datetime import datetime, timedelta

# Import VRP functions
from vrp_scheduler_v2 import supabase, JobQueue, job_queue

load_dotenv()

app = Flask(__name__)

# CORS for your frontend
CORS(app, origins=["*"])  # Restrict this in production

# ============================================================================
# HEALTH & INFO ENDPOINTS
# ============================================================================

@app.route("/")
def root():
    return jsonify({
        "status": "Profsyn VRP API with Job Queue", 
        "version": "3.0",
        "mode": "worker-based"
    })

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
    Create a VRP job (worker will pick it up and execute).
    Returns immediately with job_id for status polling.
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
        
        print(f"\n{'='*60}")
        print(f"📥 Received schedule request")
        print(f"   Inspections: {len(inspection_ids)}")
        print(f"   Dates: {target_dates}")
        print(f"   Requested by: {requested_by}")
        print(f"{'='*60}")
        
        # Clean up stale jobs first (older than 5 minutes)
        try:
            cutoff = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
            stale_result = supabase.table('job_queue').update({
                'status': 'failed',
                'error': 'Job timed out - marked as stale',
                'completed_at': datetime.utcnow().isoformat()
            }).eq('status', 'running').lt('started_at', cutoff).execute()
            
            if stale_result.data and len(stale_result.data) > 0:
                print(f"⚠️  Cleaned up {len(stale_result.data)} stale job(s)")
        except Exception as e:
            print(f"⚠️  Warning: Could not clean up stale jobs: {e}")
        
        # Check if another job is running
        if not job_queue.can_start_job():
            print("❌ Another VRP job is currently running")
            return jsonify({
                'status': 'blocked',
                'message': 'Another VRP calculation is currently running. Please wait and try again.',
                'retry_after': 30
            }), 409
        
        # Create job (worker will execute it)
        job_id = job_queue.create_job(
            job_type='vrp_calculation',
            params={
                'inspection_ids': inspection_ids,
                'target_dates': target_dates,
                'requested_by': requested_by
            }
        )
        
        if not job_id:
            print("❌ Failed to create job in queue")
            return jsonify({
                'error': 'Failed to create job in queue',
                'status': 'error'
            }), 500
        
        print(f"✅ Created job: {job_id}")
        print(f"   Status: pending (worker will process)")
        print(f"{'='*60}\n")
        
        # Return immediately - worker will process
        return jsonify({
            'status': 'pending',
            'job_id': job_id,
            'message': 'Job queued for processing. Use /api/job-status/{job_id} to check progress.'
        }), 202  # 202 Accepted
        
    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"❌ Failed to create job")
        print(f"Error: {error_msg}")
        
        return jsonify({
            'error': str(e),
            'status': 'failed'
        }), 500

@app.route("/api/job-status/<job_id>", methods=["GET"])
def check_job_status(job_id):
    """
    Check the status of a VRP job.
    Used by frontend/edge function for polling.
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

@app.route("/api/job/<job_id>", methods=["GET"])
def check_job_status_alt(job_id):
    """
    Alternative route for job status (for compatibility).
    """
    return check_job_status(job_id)

# ============================================================================
# ADMIN ENDPOINTS
# ============================================================================

@app.route("/api/admin/clear-stuck-jobs", methods=["POST"])
def clear_stuck_jobs():
    """
    Admin endpoint to manually clear stuck jobs.
    Useful if worker goes down and jobs get stuck.
    """
    try:
        result = supabase.table('job_queue').update({
            'status': 'failed',
            'error': 'Manually cleared by admin',
            'completed_at': datetime.utcnow().isoformat()
        }).in_('status', ['running', 'pending']).execute()
        
        count = len(result.data) if result.data else 0
        
        print(f"🧹 Admin cleared {count} stuck job(s)")
        
        return jsonify({
            'success': True,
            'cleared_count': count,
            'message': f'Cleared {count} stuck job(s)'
        }), 200
        
    except Exception as e:
        print(f"❌ Error clearing stuck jobs: {str(e)}")
        return jsonify({
            'error': str(e)
        }), 500

@app.route("/api/admin/job-queue-status", methods=["GET"])
def job_queue_status():
    """
    Admin endpoint to see current job queue state.
    """
    try:
        # Get all recent jobs
        result = supabase.table('job_queue')\
            .select('*')\
            .order('created_at', desc=True)\
            .limit(20)\
            .execute()
        
        jobs = result.data or []
        
        # Count by status
        status_counts = {}
        for job in jobs:
            status = job['status']
            status_counts[status] = status_counts.get(status, 0) + 1
        
        return jsonify({
            'total_jobs': len(jobs),
            'status_counts': status_counts,
            'recent_jobs': jobs[:5]  # Most recent 5
        }), 200
        
    except Exception as e:
        print(f"❌ Error getting queue status: {str(e)}")
        return jsonify({
            'error': str(e)
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
# ERROR HANDLERS
# ============================================================================

@app.errorhandler(404)
def not_found(e):
    return jsonify({
        'error': 'Endpoint not found',
        'message': str(e)
    }), 404

@app.errorhandler(500)
def internal_error(e):
    return jsonify({
        'error': 'Internal server error',
        'message': str(e)
    }), 500

# ============================================================================
# RUN THE APP
# ============================================================================

if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    print(f"\n{'='*60}")
    print(f"🚀 Starting Profsyn VRP API")
    print(f"   Port: {port}")
    print(f"   Mode: Worker-based (async)")
    print(f"   Environment: {os.getenv('RAILWAY_ENVIRONMENT', 'local')}")
    print(f"{'='*60}\n")
    
    app.run(host="0.0.0.0", port=port, debug=False)
