import time
import os
from datetime import datetime, timedelta
from threading import Thread
from http.server import HTTPServer, BaseHTTPRequestHandler
import json
from vrp_scheduler_v2 import run_vrp_for_inspections, save_vrp_results, job_queue, supabase
import traceback

# ============================================================================
# HEALTH SERVER - Keeps Railway from sleeping the worker
# ============================================================================

# Track worker status for health endpoint
worker_status = {
    'started_at': None,
    'last_poll': None,
    'jobs_processed': 0,
    'last_job_id': None,
    'last_job_status': None,
    'consecutive_errors': 0
}

class HealthHandler(BaseHTTPRequestHandler):
    """Simple HTTP handler for health checks"""
    
    def log_message(self, format, *args):
        # Suppress default logging to avoid cluttering worker logs
        pass
    
    def do_GET(self):
        if self.path == '/health' or self.path == '/':
            self.send_response(200)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            
            response = {
                'status': 'healthy',
                'service': 'vrp-worker',
                'started_at': worker_status['started_at'],
                'last_poll': worker_status['last_poll'],
                'jobs_processed': worker_status['jobs_processed'],
                'last_job_id': worker_status['last_job_id'],
                'last_job_status': worker_status['last_job_status'],
                'consecutive_errors': worker_status['consecutive_errors'],
                'timestamp': datetime.utcnow().isoformat()
            }
            self.wfile.write(json.dumps(response).encode())
        
        elif self.path == '/ping':
            # Ultra-simple endpoint for cron pings
            self.send_response(200)
            self.send_header('Content-Type', 'text/plain')
            self.end_headers()
            self.wfile.write(b'pong')
        
        else:
            self.send_response(404)
            self.send_header('Content-Type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({'error': 'Not found'}).encode())

def start_health_server():
    """Start the health server in background"""
    port = int(os.getenv('PORT', 8080))
    server = HTTPServer(('0.0.0.0', port), HealthHandler)
    print(f"🏥 Health server started on port {port}")
    server.serve_forever()

# ============================================================================
# WORKER LOOP - Processes VRP jobs from queue
# ============================================================================

def worker_loop():
    """Continuously check for pending jobs and process them"""
    worker_status['started_at'] = datetime.utcnow().isoformat()
    
    print("="*60)
    print("🔧 VRP Worker Started")
    print(f"   Time: {worker_status['started_at']}")
    print(f"   Polling interval: 3 seconds")
    print(f"   Health endpoint: http://0.0.0.0:{os.getenv('PORT', 8080)}/health")
    print("="*60)
    
    consecutive_errors = 0
    max_consecutive_errors = 10  # Increased tolerance
    
    while True:
        try:
            worker_status['last_poll'] = datetime.utcnow().isoformat()
            
            # Clean up stale jobs first (older than 5 minutes)
            try:
                cutoff = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
                stale_result = supabase.table('job_queue').update({
                    'status': 'failed',
                    'error': 'Job timed out - marked as stale by worker',
                    'completed_at': datetime.utcnow().isoformat()
                }).eq('status', 'running').lt('started_at', cutoff).execute()
                
                if stale_result.data and len(stale_result.data) > 0:
                    print(f"⚠️  Cleaned up {len(stale_result.data)} stale job(s)")
            except Exception as e:
                print(f"⚠️  Warning: Could not clean up stale jobs: {e}")
            
            # Find oldest pending job
            result = supabase.table('job_queue')\
                .select('*')\
                .eq('job_type', 'vrp_calculation')\
                .eq('status', 'pending')\
                .order('created_at', desc=False)\
                .limit(1)\
                .execute()
            
            if result.data and len(result.data) > 0:
                job = result.data[0]
                job_id = job['id']
                params = job['params']
                
                worker_status['last_job_id'] = job_id
                worker_status['last_job_status'] = 'processing'
                
                print(f"\n{'='*60}")
                print(f"📋 Picked up job: {job_id}")
                print(f"   Inspections: {len(params.get('inspection_ids', []))}")
                print(f"   Dates: {params.get('target_dates', [])}")
                print(f"{'='*60}")
                
                # Mark as running
                if not job_queue.start_job(job_id):
                    print(f"❌ Could not start job {job_id} - skipping")
                    worker_status['last_job_status'] = 'failed_to_start'
                    time.sleep(5)
                    continue
                
                # Process the job
                try:
                    print(f"🚀 Starting VRP calculation...")
                    result = run_vrp_for_inspections(
                        params['inspection_ids'],
                        params['target_dates']
                    )
                    
                    if 'error' in result:
                        job_queue.fail_job(job_id, result['error'])
                        print(f"❌ Job {job_id} failed: {result['error']}")
                        worker_status['last_job_status'] = 'failed'
                    else:
                        run_id = save_vrp_results(result['assignments'], result['metrics'])
                        job_queue.complete_job(job_id, {
                            'vrp_run_id': run_id,
                            'total_scheduled': result['metrics']['total_scheduled'],
                            'total_unscheduled': result['metrics']['total_unscheduled'],
                            'total_travel_minutes': result['metrics']['total_travel_minutes'],
                            'total_travel_km': result['metrics'].get('total_travel_km', 0),
                            'execution_seconds': result['metrics']['execution_seconds']
                        })
                        print(f"✅ Job {job_id} completed successfully")
                        print(f"   VRP Run ID: {run_id}")
                        print(f"   Scheduled: {result['metrics']['total_scheduled']} inspections")
                        print(f"   Travel: {result['metrics']['total_travel_km']:.1f} km")
                        
                        worker_status['last_job_status'] = 'completed'
                        worker_status['jobs_processed'] += 1
                        
                    # Reset error counter on success
                    consecutive_errors = 0
                    worker_status['consecutive_errors'] = 0
                        
                except Exception as e:
                    error_msg = f"{str(e)}\n{traceback.format_exc()}"
                    job_queue.fail_job(job_id, error_msg)
                    print(f"❌ Job {job_id} crashed: {error_msg}")
                    worker_status['last_job_status'] = 'crashed'
                    consecutive_errors += 1
                    worker_status['consecutive_errors'] = consecutive_errors
            else:
                # No jobs - just waiting (this is normal)
                pass
            
            # Poll every 3 seconds
            time.sleep(3)
            
        except KeyboardInterrupt:
            print("\n\n🛑 Worker stopped by user")
            break
            
        except Exception as e:
            consecutive_errors += 1
            worker_status['consecutive_errors'] = consecutive_errors
            print(f"⚠️  Worker error ({consecutive_errors}/{max_consecutive_errors}): {e}")
            print(traceback.format_exc())
            
            if consecutive_errors >= max_consecutive_errors:
                print(f"❌ Too many consecutive errors ({consecutive_errors}). Worker stopping.")
                break
                
            time.sleep(5)

# ============================================================================
# MAIN ENTRY POINT
# ============================================================================

if __name__ == "__main__":
    # Start health server in background thread (daemon=True means it dies with main thread)
    health_thread = Thread(target=start_health_server, daemon=True)
    health_thread.start()
    
    # Give health server a moment to start
    time.sleep(0.5)
    
    # Run the main worker loop
    worker_loop()
