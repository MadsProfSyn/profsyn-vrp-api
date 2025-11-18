import time
import os
from datetime import datetime, timedelta
from main import run_vrp_for_inspections, save_vrp_results, job_queue, supabase
import traceback

def worker_loop():
    """Continuously check for pending jobs and process them"""
    print("="*60)
    print("🔧 VRP Worker Started")
    print(f"   Time: {datetime.utcnow().isoformat()}")
    print(f"   Polling interval: 3 seconds")
    print("="*60)
    
    consecutive_errors = 0
    max_consecutive_errors = 5
    
    while True:
        try:
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
                
                print(f"\n{'='*60}")
                print(f"📋 Picked up job: {job_id}")
                print(f"   Inspections: {len(params.get('inspection_ids', []))}")
                print(f"   Dates: {params.get('target_dates', [])}")
                print(f"{'='*60}")
                
                # Mark as running
                if not job_queue.start_job(job_id):
                    print(f"❌ Could not start job {job_id} - skipping")
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
                        
                    # Reset error counter on success
                    consecutive_errors = 0
                        
                except Exception as e:
                    error_msg = f"{str(e)}\n{traceback.format_exc()}"
                    job_queue.fail_job(job_id, error_msg)
                    print(f"❌ Job {job_id} crashed: {error_msg}")
                    consecutive_errors += 1
            else:
                # No jobs - just waiting
                pass
            
            # Poll every 3 seconds
            time.sleep(3)
            
        except KeyboardInterrupt:
            print("\n\n🛑 Worker stopped by user")
            break
            
        except Exception as e:
            consecutive_errors += 1
            print(f"⚠️  Worker error ({consecutive_errors}/{max_consecutive_errors}): {e}")
            print(traceback.format_exc())
            
            if consecutive_errors >= max_consecutive_errors:
                print(f"❌ Too many consecutive errors ({consecutive_errors}). Worker stopping.")
                break
                
            time.sleep(5)

if __name__ == "__main__":
    worker_loop()
