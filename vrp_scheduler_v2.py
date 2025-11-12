import os
import json
from collections import defaultdict
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from datetime import datetime, timedelta
from typing import Tuple, List, Dict, Optional
import math
import pytz
from supabase import create_client, Client
from dotenv import load_dotenv
import uuid
import traceback

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

# ============================================================================
# JOB QUEUE MANAGEMENT - Prevents concurrent VRP executions
# ============================================================================

class JobQueue:
    """Manages VRP job queue to prevent concurrent executions"""
    
    def __init__(self, supabase_client: Client):
        self.supabase = supabase_client
    
    def can_start_job(self, job_type: str = "vrp_calculation") -> bool:
        """Check if a job can start (no other job running)"""
        try:
            result = self.supabase.table('job_queue').select('*')\
                .eq('job_type', job_type)\
                .eq('status', 'running')\
                .execute()
            
            return len(result.data) == 0
        except Exception as e:
            print(f"Error checking job queue: {e}")
            return False
    
    def create_job(self, job_type: str = "vrp_calculation", params: Optional[Dict] = None) -> Optional[str]:
        """Create a new job in pending state"""
        try:
            job = {
                'id': str(uuid.uuid4()),
                'job_type': job_type,
                'status': 'pending',
                'created_at': datetime.utcnow().isoformat(),
                'params': params or {}
            }
            result = self.supabase.table('job_queue').insert(job).execute()
            return result.data[0]['id']
        except Exception as e:
            print(f"Error creating job: {e}")
            return None
    
    def start_job(self, job_id: str) -> bool:
        """Mark job as running"""
        try:
            self.supabase.table('job_queue').update({
                'status': 'running',
                'started_at': datetime.utcnow().isoformat()
            }).eq('id', job_id).execute()
            return True
        except Exception as e:
            print(f"Error starting job {job_id}: {e}")
            return False
    
    def complete_job(self, job_id: str, result_data: Optional[Dict] = None) -> bool:
        """Mark job as completed with results"""
        try:
            self.supabase.table('job_queue').update({
                'status': 'completed',
                'completed_at': datetime.utcnow().isoformat(),
                'result': result_data or {}
            }).eq('id', job_id).execute()
            return True
        except Exception as e:
            print(f"Error completing job {job_id}: {e}")
            return False
    
    def fail_job(self, job_id: str, error_message: str) -> bool:
        """Mark job as failed with error message"""
        try:
            self.supabase.table('job_queue').update({
                'status': 'failed',
                'completed_at': datetime.utcnow().isoformat(),
                'error': error_message
            }).eq('id', job_id).execute()
            return True
        except Exception as e:
            print(f"Error failing job {job_id}: {e}")
            return False
    
    def get_job_status(self, job_id: str) -> Optional[Dict]:
        """Get current job status"""
        try:
            result = self.supabase.table('job_queue').select('*')\
                .eq('id', job_id)\
                .execute()
            
            if result.data and len(result.data) > 0:
                return result.data[0]
            return None
        except Exception as e:
            print(f"Error getting job status {job_id}: {e}")
            return None
    
    def cleanup_stale_jobs(self, hours: int = 24) -> int:
        """Clean up old completed/failed jobs"""
        try:
            cutoff = (datetime.utcnow() - timedelta(hours=hours)).isoformat()
            result = self.supabase.table('job_queue').delete()\
                .in_('status', ['completed', 'failed'])\
                .lt('completed_at', cutoff)\
                .execute()
            
            return len(result.data) if result.data else 0
        except Exception as e:
            print(f"Error cleaning up jobs: {e}")
            return 0

# Initialize job queue manager
job_queue = JobQueue(supabase)

# ============================================================================
# MAIN VRP EXECUTION WRAPPER - Entry point with job management
# ============================================================================

def schedule_inspections_with_queue(inspection_ids: List[str], target_dates: List[str], 
                                   requested_by: str = 'api') -> Dict:
    """
    Main entry point for scheduling inspections with job queue management.
    This prevents concurrent executions and provides status tracking.
    
    Args:
        inspection_ids: List of inspection IDs to schedule
        target_dates: List of target dates for scheduling
        requested_by: Who requested this job (for tracking)
    
    Returns:
        Dict with job_id and initial status, or error if job cannot start
    """
    
    # Clean up any stale jobs (older than 5 minutes in running state)
    print("\n🧹 Checking for stale jobs...")
    try:
        cutoff = (datetime.utcnow() - timedelta(minutes=5)).isoformat()
        stale_result = supabase.table('job_queue').update({
            'status': 'failed',
            'error': 'Job timed out - marked as stale',
            'completed_at': datetime.utcnow().isoformat()
        }).eq('status', 'running').lt('started_at', cutoff).execute()
        
        if stale_result.data and len(stale_result.data) > 0:
            print(f"⚠️ Cleaned up {len(stale_result.data)} stale jobs")
        else:
            print("✓ No stale jobs found")
    except Exception as e:
        print(f"⚠️ Warning: Could not clean up stale jobs: {e}")
    
    # Check if another job is running
    if not job_queue.can_start_job():
        print("❌ Another VRP job is currently running")
        return {
            'error': 'Another VRP calculation is currently running. Please wait and try again.',
            'status': 'blocked'
        }
    
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
        print("❌ Failed to create job in queue")
        return {
            'error': 'Failed to create job in queue',
            'status': 'error'
        }
    
    print(f"\n{'='*60}")
    print(f"🚀 Starting VRP Job: {job_id}")
    print(f"{'='*60}")
    
    try:
        # Start the job
        if not job_queue.start_job(job_id):
            print(f"❌ Failed to start job {job_id} - cleaning up")
            # Clean up the failed job attempt
            job_queue.fail_job(job_id, "Failed to start job - database connection issue")
            return {
                'error': 'Failed to start job due to database connection issue',
                'status': 'error',
                'job_id': job_id
            }
        
        print(f"✓ Job {job_id} started successfully")
        
        # Run the actual VRP calculation
        result = run_vrp_for_inspections(inspection_ids, target_dates)
        
        # Check if there was an error in VRP execution
        if 'error' in result:
            print(f"❌ VRP execution failed: {result['error']}")
            job_queue.fail_job(job_id, result['error'])
            return {
                'error': result['error'],
                'status': 'failed',
                'job_id': job_id
            }
        
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
        
        print(f"\n✅ VRP Job Completed: {job_id}")
        print(f"{'='*60}\n")
        
        return {
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
        }
        
    except Exception as e:
        error_msg = f"{str(e)}\n{traceback.format_exc()}"
        print(f"\n❌ VRP Job Failed: {job_id}")
        print(f"Error: {error_msg}")
        print(f"{'='*60}\n")
        
        job_queue.fail_job(job_id, error_msg)
        
        return {
            'error': str(e),
            'status': 'failed',
            'job_id': job_id
        }

# ============================================================================
# API ENDPOINT FUNCTIONS - For Railway deployment
# ============================================================================

def get_job_status_api(job_id: str) -> Dict:
    """
    API endpoint to check job status.
    Call this from your React app to poll for job completion.
    """
    status = job_queue.get_job_status(job_id)
    
    if not status:
        return {
            'error': 'Job not found',
            'job_id': job_id
        }
    
    return {
        'job_id': job_id,
        'status': status['status'],
        'created_at': status.get('created_at'),
        'started_at': status.get('started_at'),
        'completed_at': status.get('completed_at'),
        'result': status.get('result'),
        'error': status.get('error')
    }

# ----------------------------
# Helpers for travel estimates
# ----------------------------

def haversine_km(lat1, lng1, lat2, lng2) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlng/2)**2
    c = 2 * math.asin(math.sqrt(a))
    return R * c

def fallback_minutes(lat1, lng1, lat2, lng2) -> float:
    """
    More realistic fallback for travel time (minutes) when cache miss:
      - <=1 km: 25 km/h but min 5 min
      - <=8 km: 25 km/h
      - 8–20 km: 35 km/h
      - 20+ km: 65 km/h
    """
    km = haversine_km(lat1, lng1, lat2, lng2)
    if km <= 1.0:
        speed_kmh = 25.0
    elif km <= 8.0:
        speed_kmh = 25.0
    elif km <= 20.0:
        speed_kmh = 35.0
    else:
        speed_kmh = 65.0
    minutes = (km / speed_kmh) * 60.0
    return max(5.0, minutes)

def get_travel_time_from_cache(from_lat: float, from_lng: float,
                               to_lat: float, to_lng: float) -> float:
    """Get cached travel time or return estimate (minutes)."""
    if from_lat == to_lat and from_lng == to_lng:
        return 0.0
    key = f"{from_lng},{from_lat}->{to_lng},{to_lat}"
    result = supabase.table('mapbox_travel_cache').select('minutes').eq('key', key).execute()
    if result.data and len(result.data) > 0 and result.data[0].get('minutes') is not None:
        return max(5.0, float(result.data[0]['minutes']))
    return fallback_minutes(from_lat, from_lng, to_lat, to_lng)

def build_minutes_matrix(coords_list: List[Tuple[float, float]]) -> List[List[int]]:
    """Build travel time matrix in MINUTES (integers)."""
    size = len(coords_list)
    matrix = [[0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for j in range(size):
            if i == j:
                continue
            lat1, lng1 = coords_list[i]
            lat2, lng2 = coords_list[j]
            travel_time = get_travel_time_from_cache(lat1, lng1, lat2, lng2)
            matrix[i][j] = max(1, int(round(travel_time)))
    return matrix

def build_km_matrix(coords_list: List[Tuple[float, float]]) -> List[List[int]]:
    """Build distance matrix in deci-kilometers (integer tenths of km) for objective precision."""
    size = len(coords_list)
    matrix = [[0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for j in range(size):
            if i == j:
                continue
            lat1, lng1 = coords_list[i]
            lat2, lng2 = coords_list[j]
            km = haversine_km(lat1, lng1, lat2, lng2)
            matrix[i][j] = max(0, int(round(km * 10.0)))  # 1 unit = 0.1 km
    return matrix

def round_to_nearest_5_min(dt: datetime) -> datetime:
    """Round datetime to nearest 5 minutes (ceil)."""
    discard = timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)
    dt += timedelta(minutes=5) - discard if discard else timedelta()
    return dt.replace(second=0, microsecond=0)

# ----------------------------
# Light "area affinity" helper
# ----------------------------

def area_bucket(lat: float, lng: float, granularity_deg: float = 0.01) -> Tuple[int, int]:
    """Coarse grid (~1.1 km cells) to add small cross-bucket penalty (objective only)."""
    return (int(lat / granularity_deg), int(lng / granularity_deg))

# ---------------------------------
# Main VRP with home depots & costs
# ---------------------------------

def run_vrp_for_inspections(inspection_ids: List[str], target_dates: List[str]) -> Dict:
    """
    Objective: minimize total kilometers (Haversine), prefer fewer inspectors (route open cost).
    Feasibility/time: minutes matrix. First job at 09:00 sharp per used inspector. Return-to-home required.
    Home→first and last→home legs count in objective km, but do NOT consume day time.
    Last inspection must finish by 17:00 (soft to 17:15).
    """
    print(f"Starting VRP for {len(inspection_ids)} inspections across {len(target_dates)} dates")

    # Get inspections
    inspections_result = supabase.table('inspection_queue') \
        .select('id, address, inspection_type, rooms, preferred_date, lat, lng') \
        .in_('id', inspection_ids) \
        .eq('status', 'PENDING') \
        .execute()
    inspections = inspections_result.data or []

    # Get duration for each inspection
    for ins in inspections:
        mapping_result = supabase.table('inspection_type_mappings') \
            .select('abbreviation') \
            .eq('full_name', ins['inspection_type']) \
            .execute()
        if mapping_result.data and len(mapping_result.data) > 0:
            duration_result = supabase.table('inspection_durations') \
                .select('minutes') \
                .eq('inspection_type', mapping_result.data[0]['abbreviation']) \
                .eq('rooms', ins['rooms']) \
                .execute()
            ins['duration_minutes'] = (
                duration_result.data[0]['minutes']
                if duration_result.data and len(duration_result.data) > 0
                else 45
            )
        else:
            ins['duration_minutes'] = 45

    # Get active inspectors with homes
    inspectors_result = supabase.table('inspectors') \
        .select('id, full_name, address, lat, lng') \
        .eq('is_active', True) \
        .execute()
    inspectors_data = [
        i for i in (inspectors_result.data or [])
        if i['lat'] is not None and i['lng'] is not None
    ]

    # ============================================================================
    # CRITICAL FIX: Defensive availability time handling
    # ============================================================================
    print("\nFetching availability with defensive NULL handling...")
    availability_result = supabase.table('supabase_availability') \
        .select('inspector_id, date_local, start_time_local, end_time_local') \
        .eq('is_available', True) \
        .in_('date_local', target_dates) \
        .execute()

    avail_map: Dict[Tuple[str, str], Dict[str, str]] = {}
    for avail in (availability_result.data or []):
        key = (avail['inspector_id'], str(avail['date_local']))
        
        # Defensive: Use defaults if times are NULL
        start_time = avail.get('start_time_local')
        end_time = avail.get('end_time_local')
        
        # Handle various None/null representations
        if start_time is None or start_time == 'None' or str(start_time).lower() == 'none':
            start_time = '08:00:00'
            print(f"  ⚠️ Using default start time 08:00:00 for inspector {avail['inspector_id']}")
        
        if end_time is None or end_time == 'None' or str(end_time).lower() == 'none':
            end_time = '16:00:00'
            print(f"  ⚠️ Using default end time 16:00:00 for inspector {avail['inspector_id']}")
        
        avail_map[key] = {
            'start_time_local': str(start_time),
            'end_time_local': str(end_time)
        }
    # ============================================================================

    # Skills
    skills_result = supabase.table('inspector_skills') \
        .select('inspector_id, inspection_type') \
        .eq('is_active', True) \
        .execute()

    skills_map: Dict[str, List[str]] = {}
    for skill in (skills_result.data or []):
        skills_map.setdefault(skill['inspector_id'], []).append(skill['inspection_type'])

    # Build inspector-day slots
    inspectors: List[Dict] = []
    for insp in inspectors_data:
        for target_date in target_dates:
            key = (insp['id'], target_date)
            if key in avail_map:
                inspectors.append({
                    'inspector_id': insp['id'],
                    'full_name': insp['full_name'],
                    'home_address': insp['address'],
                    'home_lat': insp['lat'],
                    'home_lng': insp['lng'],
                    'date_local': target_date,
                    'start_time_local': avail_map[key]['start_time_local'],
                    'end_time_local': avail_map[key]['end_time_local'],
                    'can_do_types': skills_map.get(insp['id'], [])
                })

    # Load inspector capacity (with existing shifts) to avoid conflicts
    print("\nLoading inspector capacity view to avoid conflicts...")
    
    # Note: View has start_time_local/end_time_local, not day_starts/day_ends
    capacity_result = supabase.table('inspector_capacity_view') \
        .select('inspector_id, inspector_name, date_local, start_time_local, end_time_local, scheduled_shifts, booked_minutes, remaining_minutes, shift_types, capacity_status, percent_booked, shift_details') \
        .in_('date_local', target_dates) \
        .gt('scheduled_shifts', 0) \
        .execute()

    capacity_by_inspector_date: Dict[Tuple[str, str], Dict] = {}
    shifts_by_inspector_date: Dict[Tuple[str, str], List[Dict]] = {}
    
    for capacity in (capacity_result.data or []):
        key = (capacity['inspector_name'], str(capacity['date_local']))
        # Map database columns to expected internal format
        capacity_by_inspector_date[key] = {
            'inspector_id': capacity['inspector_id'],
            'inspector_name': capacity['inspector_name'],
            'date_local': capacity['date_local'],
            'day_starts': capacity['start_time_local'],  # Map column name
            'day_ends': capacity['end_time_local'],      # Map column name
            'scheduled_shifts': capacity['scheduled_shifts'],
            'booked_minutes': capacity['booked_minutes'],
            'remaining_minutes': capacity['remaining_minutes'],
            'shift_types': capacity['shift_types'],
            'capacity_status': capacity['capacity_status'],
            'percent_booked': capacity['percent_booked'],
            'shift_details': capacity['shift_details']
        }
        
        if capacity.get('shift_details'):
            shifts_by_inspector_date[key] = capacity['shift_details']

    print(f"Loaded capacity data for {len(capacity_by_inspector_date)} inspector-date combinations")

    if capacity_result.data:
        print("\nExisting shifts summary (from inspector_capacity_view):")
        for capacity in capacity_result.data:
            name = capacity['inspector_name']
            date = capacity['date_local']
            scheduled = capacity['scheduled_shifts']
            booked = capacity['booked_minutes']
            remaining = capacity['remaining_minutes']
            status = capacity['capacity_status']
            percent = capacity['percent_booked']
            
            print(f"  {name} on {date}: {scheduled} shift(s), {booked}m booked, {remaining}m remaining ({status}, {percent}% booked)")

    if not inspections:
        return {"error": "No valid inspections to schedule"}
    if not inspectors:
        return {"error": "No available inspectors for these dates"}

    print(f"Loaded {len(inspections)} inspections and {len(inspectors)} inspector-day slots")

    # Group by date
    inspections_by_date: Dict[str, List[Dict]] = {}
    for ins in inspections:
        date = ins['preferred_date']
        inspections_by_date.setdefault(date, []).append(ins)

    all_assignments: List[Dict] = []
    metrics = {
        'total_scheduled': 0,
        'total_unscheduled': 0,
        'total_travel_minutes': 0,
        'total_travel_km': 0.0,
        'execution_seconds': 0
    }

    address_by_id: Dict[str, str] = {ins['id']: ins.get('address') for ins in inspections}
    start_time = datetime.now()

    for inspection_date, date_inspections in inspections_by_date.items():
        print(f"\n{'='*60}")
        print(f"Processing {len(date_inspections)} inspections for {inspection_date}")
        print(f"{'='*60}")

        date_inspectors = [i for i in inspectors if i['date_local'] == inspection_date]
        if not date_inspectors:
            print(f"No inspectors available for {inspection_date}")
            metrics['total_unscheduled'] += len(date_inspections)
            continue

        print("\nPer-inspector availability (from capacity view):")
        total_capacity = 0
        for insp in date_inspectors:
            key = (insp['full_name'], insp['date_local'])
            
            if key in capacity_by_inspector_date:
                capacity_data = capacity_by_inspector_date[key]
                available_capacity = capacity_data['remaining_minutes']
                booked_minutes = capacity_data['booked_minutes']
                capacity_status = capacity_data['capacity_status']
                percent_booked = capacity_data['percent_booked']
                
                total_capacity += available_capacity
                
                print(f"  {insp['full_name']}: {booked_minutes}m booked, {available_capacity}m remaining ({capacity_status}, {percent_booked}% booked)")
            else:
                # ============================================================
                # DEFENSIVE: Parse times with error handling
                # ============================================================
                try:
                    start_time_str = insp.get('start_time_local', '09:00:00')
                    end_time_str = insp.get('end_time_local', '17:00:00')
                    
                    # Handle None values
                    if start_time_str is None or str(start_time_str).lower() == 'none':
                        start_time_str = '09:00:00'
                    if end_time_str is None or str(end_time_str).lower() == 'none':
                        end_time_str = '17:00:00'
                    
                    st = datetime.strptime(start_time_str, '%H:%M:%S').time()
                    et = datetime.strptime(end_time_str, '%H:%M:%S').time()
                    raw_capacity = (et.hour*60 + et.minute) - (st.hour*60 + st.minute)
                except (ValueError, TypeError, AttributeError) as e:
                    print(f"  ⚠️ WARNING: Could not parse times for {insp.get('full_name', '?')}: {e}, using defaults 09:00-17:00")
                    raw_capacity = 8 * 60  # Default 8 hours
                # ============================================================
                
                total_capacity += raw_capacity
                print(f"  {insp['full_name']}: {raw_capacity}m available (no existing shifts)")

        total_demand = sum((ins.get('duration_minutes') or 45) for ins in date_inspections if ins.get('lat') and ins.get('lng'))
        print(f"\nCapacity check for {inspection_date}:")
        print(f"  Total demand (service only): {total_demand} min")
        print(f"  Total available (after shifts): {total_capacity} min")
        if total_capacity > 0:
            print(f"  Utilization: {(total_demand / total_capacity * 100):.1f}%")

        # Build job nodes
        inspection_nodes: List[Dict] = []
        job_coords: List[Tuple[float, float]] = []
        job_area_bucket: List[Tuple[int, int]] = []
        for ins in date_inspections:
            if ins['lat'] and ins['lng']:
                inspection_nodes.append(ins)
                job_coords.append((ins['lat'], ins['lng']))
                job_area_bucket.append(area_bucket(ins['lat'], ins['lng']))
            else:
                print(f"Skipping inspection at {ins.get('address') or '?'} - missing coordinates")
                metrics['total_unscheduled'] += 1

        if not inspection_nodes:
            continue

        n_jobs = len(inspection_nodes)
        print(f"\nNode count (no dedup): {n_jobs}")

        # Homes
        home_coords: List[Tuple[float, float]] = [(i['home_lat'], i['home_lng']) for i in date_inspectors]
        home_area_bucket: List[Tuple[int, int]] = [area_bucket(i['home_lat'], i['home_lng']) for i in date_inspectors]

        # Combined
        coords: List[Tuple[float, float]] = home_coords + job_coords
        area_buckets: List[Tuple[int, int]] = home_area_bucket + job_area_bucket

        print("Building travel matrices (minutes + km)...")
        minutes_matrix = build_minutes_matrix(coords)
        kmdeci_matrix = build_km_matrix(coords)

        num_vehicles = len(date_inspectors)
        HOME_OFFSET = 0
        JOB_OFFSET = num_vehicles
        n = num_vehicles + n_jobs

        starts = list(range(HOME_OFFSET, HOME_OFFSET + num_vehicles))
        ends   = list(range(HOME_OFFSET, HOME_OFFSET + num_vehicles))

        mgr = pywrapcp.RoutingIndexManager(n, num_vehicles, starts, ends)
        routing = pywrapcp.RoutingModel(mgr)

        # Service durations array aligned with global node index
        durations = [0] * num_vehicles
        for j, ins in enumerate(inspection_nodes):
            durations.append(ins.get('duration_minutes') or 45)

        # Time callback
        def time_callback(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            to_node = mgr.IndexToNode(to_idx)

            travel_minutes = 0
            if from_node >= JOB_OFFSET and to_node >= JOB_OFFSET:
                travel_minutes = minutes_matrix[from_node][to_node]

            return travel_minutes + durations[from_node]

        time_cb = routing.RegisterTransitCallback(time_callback)

        # Routing objective: km only
        AREA_JUMP_PENALTY_DECIKM = 2
        def km_cost_callback(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            to_node = mgr.IndexToNode(to_idx)
            base = kmdeci_matrix[from_node][to_node]
            if area_buckets[from_node] != area_buckets[to_node]:
                return base + AREA_JUMP_PENALTY_DECIKM
            return base

        km_cb = routing.RegisterTransitCallback(km_cost_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(km_cb)

        # Prefer fewer inspectors
        ROUTE_OPEN_COST_DECIKM = 500
        for v in range(num_vehicles):
            routing.SetFixedCostOfVehicle(ROUTE_OPEN_COST_DECIKM, v)

        # Time dimension
        print("\nApplying 09:00 start and end-by-17:00 (soft 17:15) constraints...")
        time_horizon = 24 * 60
        MAX_WAIT_PER_LEG = 8 * 60
        routing.AddDimension(
            time_cb,
            MAX_WAIT_PER_LEG,
            time_horizon,
            False,
            'Time'
        )
        time_dim = routing.GetDimensionOrDie('Time')

        def to_minutes(t: datetime.time) -> int:
            return t.hour * 60 + t.minute

        HARD_START = 9 * 60
        HARD_END   = 17 * 60
        SOFT_END   = 17 * 60 + 15
        OVERTIME_COST_PER_MIN = 300

        print("\nApplying time windows (adjusted for existing shifts):")
        for v, insp in enumerate(date_inspectors):
            start_index = routing.Start(v)
            end_index   = routing.End(v)

            # ============================================================
            # DEFENSIVE: Parse availability times with error handling
            # ============================================================
            try:
                start_time_str = insp.get('start_time_local', '09:00:00')
                end_time_str = insp.get('end_time_local', '17:00:00')
                
                # Handle None values
                if start_time_str is None or str(start_time_str).lower() == 'none':
                    start_time_str = '09:00:00'
                if end_time_str is None or str(end_time_str).lower() == 'none':
                    end_time_str = '17:00:00'
                
                st = datetime.strptime(start_time_str, '%H:%M:%S').time()
                et = datetime.strptime(end_time_str, '%H:%M:%S').time()
            except (ValueError, TypeError, AttributeError) as e:
                print(f"  ⚠️ WARNING: Could not parse times for {insp.get('full_name', '?')}: {e}, using defaults")
                st = datetime.strptime('09:00:00', '%H:%M:%S').time()
                et = datetime.strptime('17:00:00', '%H:%M:%S').time()
            
            start_min = st.hour * 60 + st.minute
            end_min = et.hour * 60 + et.minute
            original_start = start_min
            # ============================================================

            # Check for existing shifts and adjust start time using capacity view
            key = (insp['full_name'], insp['date_local'])
            if key in shifts_by_inspector_date:
                inspector_shifts = shifts_by_inspector_date[key]
                
                # Find the latest shift end time
                latest_shift_end_min = 0
                for shift in inspector_shifts:
                    # ============================================================
                    # DEFENSIVE: Skip shifts with missing end_time
                    # ============================================================
                    if not shift.get('end_time'):
                        print(f"    ⚠️ WARNING: Shift missing end_time for {insp['full_name']}, skipping")
                        continue
                        
                    try:
                        shift_end = datetime.strptime(shift['end_time'], '%H:%M:%S').time()
                        shift_end_min = shift_end.hour * 60 + shift_end.minute
                        latest_shift_end_min = max(latest_shift_end_min, shift_end_min)
                    except (ValueError, TypeError) as e:
                        print(f"    ⚠️ WARNING: Could not parse shift end_time '{shift.get('end_time')}': {e}")
                        continue
                    # ============================================================
                
                # Adjust start time to after latest shift (with 15 min buffer)
                if latest_shift_end_min > start_min:
                    start_min = latest_shift_end_min + 15  # 15 min buffer
                    print(f"  {insp['full_name']}: Adjusted start from {original_start//60:02d}:{original_start%60:02d} to {start_min//60:02d}:{start_min%60:02d} (after shifts ending at {latest_shift_end_min//60:02d}:{latest_shift_end_min%60:02d})")
            
            # Ensure start time doesn't exceed end time
            if start_min >= end_min:
                print(f"  WARNING: {insp['full_name']} has no available time after shifts")
                start_min = end_min - 30  # Minimal window

            # Enforce start min
            avail_start = start_min
            start_min = max(HARD_START, avail_start if avail_start else HARD_START)
            time_dim.CumulVar(start_index).SetRange(start_min, start_min)

            # Soft end constraint
            hard_end_cap = min(end_min if end_min else SOFT_END, SOFT_END)
            time_dim.CumulVar(end_index).SetRange(start_min, hard_end_cap)
            time_dim.SetCumulVarSoftUpperBound(end_index, HARD_END, OVERTIME_COST_PER_MIN)

            routing.AddVariableMinimizedByFinalizer(time_dim.CumulVar(end_index))
            routing.AddVariableMinimizedByFinalizer(time_dim.CumulVar(start_index))

        # Skill constraints
        print("\nApplying skill constraints...")
        for j, ins in enumerate(inspection_nodes):
            node_index = mgr.NodeToIndex(JOB_OFFSET + j)
            allowed: List[int] = []
            for vehicle_id, insp in enumerate(date_inspectors):
                if ins['inspection_type'] in insp['can_do_types']:
                    allowed.append(vehicle_id)
            if allowed:
                routing.SetAllowedVehiclesForIndex(allowed, node_index)
                addr = (ins.get('address') or '')[:40]
                print(f"  {ins['inspection_type']} @ {addr} → vehicles {allowed}")
            else:
                print(f"  WARNING: No qualified inspector for {ins['inspection_type']}")

        # Big penalty for dropping jobs
        BIG_DROP_PENALTY = 1_000_000_000
        for node in range(JOB_OFFSET, JOB_OFFSET + n_jobs):
            routing.AddDisjunction([mgr.NodeToIndex(node)], BIG_DROP_PENALTY)

        # Search params
        search = pywrapcp.DefaultRoutingSearchParameters()
        search.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search.time_limit.seconds = 90

        print(f"\nSolving VRP with {num_vehicles} inspectors and {n_jobs} jobs (minimize km; start/end at home)...")
        solution = routing.SolveWithParameters(search)
        if not solution:
            print("No complete solution found")
            metrics['total_unscheduled'] += n_jobs
            continue

        print("✓ Solution found!")

        # Dropped diagnostics
        dropped = []
        for node in range(JOB_OFFSET, JOB_OFFSET + n_jobs):
            node_index = mgr.NodeToIndex(node)
            if solution.Value(routing.NextVar(node_index)) == node_index:
                dropped.append(inspection_nodes[node - JOB_OFFSET])
        if dropped:
            print("\nDropped inspections (despite huge penalty):")
            for ins in dropped:
                addr = (ins.get('address') or '')[:50]
                print(f"  - {ins['inspection_type']} @ {addr}")

        # Extract solution with corrected travel time attribution
        tz = pytz.timezone('Europe/Copenhagen')
        base_date = datetime.strptime(inspection_date, '%Y-%m-%d').date()
        day_midnight = tz.localize(datetime.combine(base_date, datetime.min.time()))

        total_km_deci = 0
        for v in range(num_vehicles):
            insp = date_inspectors[v]
            name = insp['full_name']

            index = routing.Start(v)
            if routing.IsEnd(solution.Value(routing.NextVar(index))):
                continue

            # Build route
            route_nodes: List[int] = []
            prev_index = index
            while not routing.IsEnd(prev_index):
                prev_index = solution.Value(routing.NextVar(prev_index))
                node = mgr.IndexToNode(prev_index)
                route_nodes.append(node)

            # From HOME -> first (km only)
            prev_node = starts[v]
            if route_nodes and route_nodes[0] >= JOB_OFFSET:
                total_km_deci += kmdeci_matrix[prev_node][route_nodes[0]]

            # ============================================================
            # DEFENSIVE: Parse times for solution extraction
            # ============================================================
            try:
                start_time_str = insp.get('start_time_local', '09:00:00')
                if start_time_str is None or str(start_time_str).lower() == 'none':
                    start_time_str = '09:00:00'
                st = datetime.strptime(start_time_str, '%H:%M:%S').time()
                start_min = st.hour * 60 + st.minute
            except (ValueError, TypeError, AttributeError):
                start_min = 9 * 60  # Default 09:00
            # ============================================================
            
            # Adjust for shifts
            key = (insp['inspector_id'], insp['date_local'])
            if key in shifts_by_inspector_date:
                latest_end = 0
                for shift in shifts_by_inspector_date[key]:
                    # ============================================================
                    # DEFENSIVE: Skip shifts with missing end_time_local
                    # ============================================================
                    if not shift.get('end_time_local'):
                        continue
                        
                    try:
                        shift_end = datetime.strptime(shift['end_time_local'], '%H:%M:%S').time()
                        shift_end_min = shift_end.hour * 60 + shift_end.minute
                        latest_end = max(latest_end, shift_end_min)
                    except (ValueError, TypeError):
                        continue
                    # ============================================================
                    
                if latest_end > start_min:
                    start_min = latest_end + 15
            
            first_start_min = max(9 * 60, start_min)
            current_min = first_start_min

            sequence = 0
            prev_node = starts[v]  # Initialize prev_node to home
            
            # Iterate through jobs
            for node in route_nodes:
                if node < JOB_OFFSET:
                    prev_node = node
                    continue

                job = inspection_nodes[node - JOB_OFFSET]
                
                # ============================================================
                # CHANGE: Skip travel time for first job (starts at 09:00)
                # ============================================================
                if sequence == 0:
                    # First job: starts at inspector's available time, no travel time added
                    travel_min_to_here = 0
                elif prev_node >= JOB_OFFSET:
                    # Subsequent jobs: calculate travel time from previous job
                    travel_min_to_here = minutes_matrix[prev_node][node]
                    if travel_min_to_here == 0:
                        travel_min_to_here = 5  # Minimum buffer
                else:
                    # Should not happen for non-first jobs
                    travel_min_to_here = minutes_matrix[prev_node][node]
                # ============================================================

                # Add travel time BEFORE starting this job (0 for first job)
                current_min += travel_min_to_here

                # Schedule start (rounded to 5 min)
                start_dt = day_midnight + timedelta(minutes=current_min)
                start_dt = round_to_nearest_5_min(start_dt)
                current_min = (start_dt - day_midnight).seconds // 60

                # Service duration
                duration = job.get('duration_minutes') or 45
                end_minute = current_min + duration

                # Move time forward by service duration ONLY
                current_min = end_minute

                sequence += 1
                all_assignments.append({
                    'inspection_id': job['id'],
                    'inspector_id': insp['inspector_id'],
                    'scheduled_date': inspection_date,
                    'start_time': start_dt.time().isoformat(),
                    'end_time': (day_midnight + timedelta(minutes=end_minute)).time().isoformat(),
                    'sequence_in_route': sequence,
                    'travel_from_previous_mins': travel_min_to_here
                })

                print(f"  {name}: Seq {sequence}: {job['inspection_type']} @ {job.get('address', '?')[:40]} | {start_dt.strftime('%H:%M')}-{(day_midnight + timedelta(minutes=end_minute)).strftime('%H:%M')} | travel from prev: {travel_min_to_here}m, service: {duration}m")

                metrics['total_scheduled'] += 1
                metrics['total_travel_minutes'] += travel_min_to_here

                prev_node = node

            # Job->home travel (km only)
            last_job_node = prev_node
            total_km_deci += kmdeci_matrix[last_job_node][ends[v]]

            # Sum km arcs
            km_prev = starts[v]
            for node in route_nodes:
                total_km_deci += kmdeci_matrix[km_prev][node]
                km_prev = node

            # Print return home
            if route_nodes:
                last_addr = inspection_nodes[route_nodes[-1] - JOB_OFFSET].get('address') if route_nodes[-1] >= JOB_OFFSET else "HOME"
                back_km = kmdeci_matrix[route_nodes[-1]][ends[v]] / 10.0
                print(f"  {name}: → HOME from {last_addr} ({back_km:.1f} km)")

        metrics['total_travel_km'] += total_km_deci / 10.0

        # Per-inspector summary
        per_insp = defaultdict(lambda: {"count": 0, "travel_min": 0})
        for a in [a for a in all_assignments if a["scheduled_date"] == inspection_date]:
            per_insp[a["inspector_id"]]["count"] += 1
            per_insp[a["inspector_id"]]["travel_min"] += a["travel_from_previous_mins"]

        id_to_name = {i["inspector_id"]: i["full_name"] for i in date_inspectors}
        print("\nActual assignments per inspector (this date):")
        for insp in date_inspectors:
            pid = insp["inspector_id"]
            print(f"  {id_to_name[pid]} → {per_insp[pid]['count']} visits (job→job travel {per_insp[pid]['travel_min']} min)")

        # Itineraries
        print("\nDetailed itineraries (with addresses):")
        for insp in date_inspectors:
            pid = insp["inspector_id"]
            person_jobs = [a for a in all_assignments if a["scheduled_date"] == inspection_date and a["inspector_id"] == pid]
            person_jobs.sort(key=lambda x: x["sequence_in_route"])
            print(f"  {insp['full_name']}:")
            if not person_jobs:
                print("    (no assignments)")
            else:
                for a in person_jobs:
                    addr_str = address_by_id.get(a["inspection_id"], "?")
                    print(f"    #{a['sequence_in_route']:02d} {a['start_time']}–{a['end_time']} | {addr_str} (travel {a['travel_from_previous_mins']}m)")

    metrics['execution_seconds'] = (datetime.now() - start_time).total_seconds()
    metrics['total_unscheduled'] = len(inspections) - metrics['total_scheduled']

    print(f"\n{'='*60}")
    print(f"VRP Complete: {metrics['total_scheduled']} scheduled, {metrics['total_unscheduled']} unscheduled")
    print(f"Total travel time: {metrics['total_travel_minutes']} minutes")
    print(f"Total travel distance (incl. home legs): {metrics['total_travel_km']:.1f} km")
    print(f"Execution time: {metrics['execution_seconds']:.1f} seconds")
    print(f"{'='*60}")

    return {
        'assignments': all_assignments,
        'metrics': metrics
    }

def save_vrp_results(assignments, metrics):
    """Save VRP assignments to proposed_assignments table"""
    run_id = str(uuid.uuid4())

    supabase.table('vrp_runs').insert({
        'id': run_id,
        'inspection_ids': [a['inspection_id'] for a in assignments],
        'target_dates': list(set(a['scheduled_date'] for a in assignments)),
        'status': 'COMPLETED',
        'num_inspections_scheduled': metrics['total_scheduled'],
        'total_travel_minutes': metrics['total_travel_minutes'],
        'execution_seconds': metrics['execution_seconds'],
        'requested_by': 'api',
        'triggered_by': 'api',
        'total_travel_km': metrics.get('total_travel_km', None),
    }).execute()

    for assignment in assignments:
        supabase.table('proposed_assignments').insert({
            'vrp_run_id': run_id,
            **assignment
        }).execute()

    return run_id

if __name__ == "__main__":
    # Test with job queue management
    test_inspection_ids = [
        'a34b89e0-0539-43ed-82dc-05593190a8ab',
        'd807c526-548f-4ab7-a27b-421d383cdd27'
    ]
    test_dates = ['2025-10-15']
    
    # Use the new queue-managed function
    result = schedule_inspections_with_queue(test_inspection_ids, test_dates, requested_by='test')
    print(json.dumps(result, indent=2))
