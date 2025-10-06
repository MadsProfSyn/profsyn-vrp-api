import os
import json
from collections import defaultdict
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict
import math
import pytz
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

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
    More realistic fallback:
      - very short hops (<= 1 km): ~ 3–5 km/h walking/elevator/lobby friction -> ~12–20 min/km unrealistic for vehicle
        but these are often same-building; keep a minimum of 5 min.
      - city hops (<= 8 km): use ~ 22–28 km/h avg (signals/parking/etc) -> 25 km/h
      - cross-town (8–20 km): ~ 35 km/h
      - longer (20+ km): ~ 65 km/h (motorways)
    """
    km = haversine_km(lat1, lng1, lat2, lng2)
    if km <= 1.0:
        speed_kmh = 25.0  # keep vehicle-speed assumption; min guard below will enforce realistic floor
    elif km <= 8.0:
        speed_kmh = 25.0
    elif km <= 20.0:
        speed_kmh = 35.0
    else:
        speed_kmh = 65.0
    minutes = (km / speed_kmh) * 60.0
    return max(5.0, minutes)  # never below 5 min

def get_travel_time_from_cache(from_lat: float, from_lng: float, 
                               to_lat: float, to_lng: float) -> float:
    """Get cached travel time or return estimate"""
    if from_lat == to_lat and from_lng == to_lng:
        return 0.0
    key = f"{from_lng},{from_lat}->{to_lng},{to_lat}"
    result = supabase.table('mapbox_travel_cache').select('minutes').eq('key', key).execute()
    if result.data and len(result.data) > 0 and result.data[0].get('minutes') is not None:
        # Enforce a floor; cached times can be unrealistically low sometimes
        return max(5.0, float(result.data[0]['minutes']))
    # Fallback
    return fallback_minutes(from_lat, from_lng, to_lat, to_lng)

def build_distance_matrix(coords_list: List[Tuple[float, float]]) -> List[List[int]]:
    """Build travel time matrix in MINUTES (integers)"""
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

def round_to_nearest_5_min(dt: datetime) -> datetime:
    """Round datetime to nearest 5 minutes"""
    discard = timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)
    dt += timedelta(minutes=5) - discard if discard else timedelta()
    return dt.replace(second=0, microsecond=0)

# ----------------------------
# Light "area affinity" helper
# ----------------------------

def area_bucket(lat: float, lng: float, granularity_deg: float = 0.01) -> Tuple[int, int]:
    """
    Bucket coordinates into a coarse grid (~0.01 deg ≈ ~1.1 km).
    Used to add a small cost penalty when routes jump to a different bucket.
    """
    return (int(lat / granularity_deg), int(lng / granularity_deg))

# ---------------------------------
# Main VRP with home depots & costs
# ---------------------------------

def run_vrp_for_inspections(inspection_ids: List[str], target_dates: List[str]) -> Dict:
    """Main VRP function adapted for new schema with:
       - home depots (start & end)
       - separate time and cost callbacks
       - area-jump cost penalty
       - more realistic fallback speeds
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

    # Get available inspectors
    inspectors_result = supabase.table('inspectors') \
        .select('id, full_name, address, lat, lng') \
        .eq('is_active', True) \
        .execute()
    inspectors_data = [
        i for i in (inspectors_result.data or [])
        if i['lat'] is not None and i['lng'] is not None
    ]

    # Join with availability
    availability_result = supabase.table('supabase_availability') \
        .select('inspector_id, date_local, start_time_local, end_time_local') \
        .eq('is_available', True) \
        .in_('date_local', target_dates) \
        .execute()

    # Create inspector availability map
    avail_map: Dict[Tuple[str, str], Dict[str, str]] = {}
    for avail in (availability_result.data or []):
        key = (avail['inspector_id'], str(avail['date_local']))
        avail_map[key] = {
            'start_time_local': str(avail['start_time_local']),
            'end_time_local': str(avail['end_time_local'])
        }

    # Get skills for each inspector
    skills_result = supabase.table('inspector_skills') \
        .select('inspector_id, inspection_type') \
        .eq('is_active', True) \
        .execute()

    skills_map: Dict[str, List[str]] = {}
    for skill in (skills_result.data or []):
        if skill['inspector_id'] not in skills_map:
            skills_map[skill['inspector_id']] = []
        skills_map[skill['inspector_id']].append(skill['inspection_type'])

    # Build inspectors array with availability and skills
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

    if not inspections:
        return {"error": "No valid inspections to schedule"}
    if not inspectors:
        return {"error": "No available inspectors for these dates"}

    print(f"Loaded {len(inspections)} inspections and {len(inspectors)} inspector-day slots")

    # Group by date
    inspections_by_date: Dict[str, List[Dict]] = {}
    for ins in inspections:
        date = ins['preferred_date']
        if date not in inspections_by_date:
            inspections_by_date[date] = []
        inspections_by_date[date].append(ins)

    all_assignments: List[Dict] = []
    metrics = {
        'total_scheduled': 0,
        'total_unscheduled': 0,
        'total_travel_minutes': 0,
        'execution_seconds': 0
    }

    # For printing detailed itineraries with addresses later
    address_by_id: Dict[str, str] = {ins['id']: ins.get('address') for ins in inspections}

    start_time = datetime.now()

    # Process each date separately
    for inspection_date, date_inspections in inspections_by_date.items():
        print(f"\n{'='*60}")
        print(f"Processing {len(date_inspections)} inspections for {inspection_date}")
        print(f"{'='*60}")

        date_inspectors = [i for i in inspectors if i['date_local'] == inspection_date]
        if not date_inspectors:
            print(f"No inspectors available for {inspection_date}")
            metrics['total_unscheduled'] += len(date_inspections)
            continue

        # --- Capacity sanity check ---
        total_capacity = 0
        print("\nPer-inspector window lengths (minutes):")
        for insp in date_inspectors:
            st = datetime.strptime(insp['start_time_local'], '%H:%M:%S').time()
            et = datetime.strptime(insp['end_time_local'], '%H:%M:%S').time()
            cap = (et.hour*60 + et.minute) - (st.hour*60 + st.minute)
            total_capacity += cap
            print(f"  {insp['full_name']}: {cap} min")

        total_demand = sum((ins.get('duration_minutes') or 45) for ins in date_inspections if ins.get('lat') and ins.get('lng'))
        print(f"Capacity check for {inspection_date}: demand={total_demand} min, supply={total_capacity} min (not counting travel)")

        # Build per-inspection nodes (no dedup)
        inspection_nodes: List[Dict] = []
        job_coords: List[Tuple[float, float]] = []
        job_area_bucket: List[Tuple[int, int]] = []
        for ins in date_inspections:
            if ins['lat'] and ins['lng']:
                inspection_nodes.append(ins)                  # keep each inspection as its own node
                job_coords.append((ins['lat'], ins['lng']))   # 1:1 with nodes
                job_area_bucket.append(area_bucket(ins['lat'], ins['lng']))
            else:
                print(f"Skipping inspection at {ins['address']} - missing coordinates")
                metrics['total_unscheduled'] += 1

        if not inspection_nodes:
            continue

        n_jobs = len(inspection_nodes)
        print(f"\nNode count (no dedup): {n_jobs} (should equal number of valid inspections for this date)")

        # Build list of home depots (one per inspector)
        home_coords: List[Tuple[float, float]] = [(i['home_lat'], i['home_lng']) for i in date_inspectors]
        home_area_bucket: List[Tuple[int, int]] = [area_bucket(i['home_lat'], i['home_lng']) for i in date_inspectors]

        # Combined coordinate list: homes first, then jobs
        coords: List[Tuple[float, float]] = home_coords + job_coords
        # Area buckets aligned with coords
        area_buckets: List[Tuple[int, int]] = home_area_bucket + job_area_bucket

        print("Building travel matrix (homes + jobs)...")
        matrix = build_distance_matrix(coords)

        # Indexing
        num_vehicles = len(date_inspectors)
        HOME_OFFSET = 0                      # homes: [0 .. num_vehicles-1]
        JOB_OFFSET = num_vehicles            # jobs:  [num_vehicles .. num_vehicles + n_jobs - 1]
        n_real = num_vehicles + n_jobs

        # Add dummy end to close routes if you prefer not to return to home
        dummy_end = n_real
        for row in matrix:
            row.append(0)
        matrix.append([0] * (n_real + 1))
        n = n_real + 1

        # OR-Tools starts/ends: start at each home, end at dummy (or set to home index to return-to-home)
        starts = list(range(HOME_OFFSET, HOME_OFFSET + num_vehicles))
        ends = [dummy_end] * num_vehicles

        mgr = pywrapcp.RoutingIndexManager(n, num_vehicles, starts, ends)
        routing = pywrapcp.RoutingModel(mgr)

        # Service durations: homes=0, jobs=duration
        durations = [0] * num_vehicles
        for j, ins in enumerate(inspection_nodes):
            durations.append(ins.get('duration_minutes') or 45)  # aligned with JOB_OFFSET + j
        durations.append(0)  # dummy

        # --------------------------
        # Separate TIME vs COST cbs
        # --------------------------
        def time_callback(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            to_node = mgr.IndexToNode(to_idx)
            # Pure time for the Time dimension: travel + service at FROM node
            return matrix[from_node][to_node] + durations[from_node]

        time_cb = routing.RegisterTransitCallback(time_callback)

        AREA_JUMP_PENALTY = 8  # in "cost minutes" (tune: 5-15). Does NOT inflate time dimension.
        def cost_callback(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            to_node = mgr.IndexToNode(to_idx)
            base = matrix[from_node][to_node] + durations[from_node]
            # add soft penalty when jumping between different grid buckets
            if area_buckets[from_node] != area_buckets[to_node]:
                return base + AREA_JUMP_PENALTY
            return base

        cost_cb = routing.RegisterTransitCallback(cost_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(cost_cb)

        # -------------------
        # Time dimension
        # -------------------
        print("\nInspector availability windows:")
        time_windows: List[Tuple[int, int]] = []
        for insp in date_inspectors:
            st = datetime.strptime(insp['start_time_local'], '%H:%M:%S').time()
            et = datetime.strptime(insp['end_time_local'], '%H:%M:%S').time()
            start_min = st.hour * 60 + st.minute
            end_min = et.hour * 60 + et.minute
            time_windows.append((start_min, end_min))
            print(f"  {insp['full_name']}: {insp['start_time_local']} - {insp['end_time_local']} ({start_min}-{end_min} min)")

        day_horizon = 24 * 60
        routing.AddDimension(
            time_cb,        # use the PURE time callback
            0,              # no extra waiting slack
            day_horizon,    # max cumul
            False,
            'Time'
        )
        time_dimension = routing.GetDimensionOrDie('Time')

        # Allow soft overtime on the vehicle end
        OVERTIME_CAP_MIN = 240        # up to +4h if needed
        OVERTIME_COST_PER_MIN = 200   # overtime is expensive but cheaper than drop

        for vehicle_id, (start_min, end_min) in enumerate(time_windows):
            start_index = routing.Start(vehicle_id)
            end_index   = routing.End(vehicle_id)

            time_dimension.CumulVar(start_index).SetRange(start_min, start_min)
            time_dimension.CumulVar(end_index).SetRange(start_min, end_min + OVERTIME_CAP_MIN)
            time_dimension.SetCumulVarSoftUpperBound(end_index, end_min, OVERTIME_COST_PER_MIN)

            routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(end_index))
            routing.AddVariableMinimizedByFinalizer(time_dimension.CumulVar(start_index))

        # -------------
        # Load balance (optional, unchanged)
        # -------------
        def service_only_cb(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            return durations[from_node]  # only service duration, no travel

        svc_cb = routing.RegisterTransitCallback(service_only_cb)
        routing.AddDimension(
            svc_cb,
            0,
            24 * 60,
            True,      # start at 0
            "ServiceLoad"
        )
        svc_dim = routing.GetDimensionOrDie("ServiceLoad")
        LOAD_CAP = 300
        LOAD_PENALTY = 100   # a bit stronger to avoid 6-0 splits
        for v in range(num_vehicles):
            end_idx = routing.End(v)
            svc_dim.SetCumulVarSoftUpperBound(end_idx, LOAD_CAP, LOAD_PENALTY)

        # ------------------------
        # Skill constraints on JOBS
        # ------------------------
        print("\nApplying skill constraints...")
        for j, ins in enumerate(inspection_nodes):
            node_index = mgr.NodeToIndex(JOB_OFFSET + j)
            allowed_vehicles: List[int] = []
            for vehicle_id, insp in enumerate(date_inspectors):
                if ins['inspection_type'] in insp['can_do_types']:
                    allowed_vehicles.append(vehicle_id)
            if allowed_vehicles:
                routing.SetAllowedVehiclesForIndex(allowed_vehicles, node_index)
                addr = (ins.get('address') or '')[:30]
                print(f"  {ins['inspection_type']} at {addr}... → inspectors {allowed_vehicles}")
            else:
                print(f"  WARNING: No qualified inspector for {ins['inspection_type']}")

        # BIG penalties for dropping any JOB node (not home or dummy)
        BIG_DROP_PENALTY = 1_000_000
        for node in range(JOB_OFFSET, JOB_OFFSET + n_jobs):
            routing.AddDisjunction([mgr.NodeToIndex(node)], BIG_DROP_PENALTY)

        # Solve with increased timeout
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 300

        print(f"\nSolving VRP with {num_vehicles} inspectors and {n_jobs} nodes (1 per inspection, home depots enabled)...")
        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            print("No complete solution found")
            metrics['total_unscheduled'] += n_jobs
            continue

        print("✓ Solution found!")

        # --- Dropped node diagnostics ---
        dropped = []
        for node in range(JOB_OFFSET, JOB_OFFSET + n_jobs):
            node_index = mgr.NodeToIndex(node)
            if solution.Value(routing.NextVar(node_index)) == node_index:
                dropped.append(inspection_nodes[node - JOB_OFFSET])

        if dropped:
            print("\nDropped inspections (solver skipped despite penalty):")
            for ins in dropped:
                eligible = []
                for vid, insp in enumerate(date_inspectors):
                    if ins['inspection_type'] in insp['can_do_types']:
                        eligible.append(insp['full_name'])
                reason = "NO QUALIFIED INSPECTOR" if not eligible else "LIKELY TIME CAPACITY / ROUTING"
                addr = (ins.get('address') or '')[:40]
                print(f"  - {ins['inspection_type']} | rooms={ins.get('rooms')} | addr={addr}... | eligible={len(eligible)} -> {reason}")

        # Extract solution
        tz = pytz.timezone('Europe/Copenhagen')
        base_date = datetime.strptime(inspection_date, '%Y-%m-%d').date()
        day_midnight = tz.localize(datetime.combine(base_date, datetime.min.time()))

        for vehicle_id in range(num_vehicles):
            insp = date_inspectors[vehicle_id]
            index = routing.Start(vehicle_id)
            route_sequence = 0
            prev_node = None

            while not routing.IsEnd(index):
                node = mgr.IndexToNode(index)

                # Skip homes and dummy in printout; we only print jobs
                if node < JOB_OFFSET or node == dummy_end:
                    index = solution.Value(routing.NextVar(index))
                    continue

                # Job node
                ins = inspection_nodes[node - JOB_OFFSET]

                # Minutes from midnight
                time_var = time_dimension.CumulVar(index)
                time_min = solution.Value(time_var)

                start_dt = day_midnight + timedelta(minutes=time_min)
                start_dt = round_to_nearest_5_min(start_dt)

                duration = ins.get('duration_minutes') or 45
                end_dt = start_dt + timedelta(minutes=duration)

                # Travel time between nodes (matrix minutes)
                travel_time = 0
                if prev_node is not None:
                    travel_time = matrix[prev_node][node]

                route_sequence += 1
                prev_node = node

                addr_str = ins.get('address') or '?'
                print(f"  {insp['full_name']}: {ins['inspection_type']} @ {addr_str} at {start_dt.strftime('%H:%M')} (travel: {travel_time}min)")

                all_assignments.append({
                    'inspection_id': ins['id'],
                    'inspector_id': insp['inspector_id'],
                    'date': inspection_date,
                    'start_time': start_dt.time().isoformat(),
                    'end_time': end_dt.time().isoformat(),
                    'sequence': route_sequence,
                    'travel_mins': travel_time
                })

                metrics['total_scheduled'] += 1
                metrics['total_travel_minutes'] += travel_time

                index = solution.Value(routing.NextVar(index))

        # --- Assignment summary per inspector (for this date) ---
        per_insp = defaultdict(lambda: {"count": 0, "travel": 0})
        for a in [a for a in all_assignments if a["date"] == inspection_date]:
            per_insp[a["inspector_id"]]["count"] += 1
            per_insp[a["inspector_id"]]["travel"] += a["travel_mins"]

        id_to_name = {i["inspector_id"]: i["full_name"] for i in date_inspectors}
        print("\nActual assignments per inspector (this date):")
        for insp in date_inspectors:
            pid = insp["inspector_id"]
            print(f"  {id_to_name[pid]} → {per_insp[pid]['count']} visits (travel {per_insp[pid]['travel']} min)")

        # --- Detailed itineraries with addresses (this date) ---
        print("\nDetailed itineraries (with addresses):")
        for insp in date_inspectors:
            pid = insp["inspector_id"]
            person_jobs = [a for a in all_assignments if a["date"] == inspection_date and a["inspector_id"] == pid]
            person_jobs.sort(key=lambda x: x["sequence"])
            print(f"  {insp['full_name']}:")
            if not person_jobs:
                print("    (no assignments)")
                continue
            for a in person_jobs:
                addr_str = address_by_id.get(a["inspection_id"], "?")
                print(f"    #{a['sequence']:02d} {a['start_time']}–{a['end_time']} | {addr_str} (travel {a['travel_mins']}m)")

    metrics['execution_seconds'] = (datetime.now() - start_time).total_seconds()
    metrics['total_unscheduled'] = len(inspections) - metrics['total_scheduled']

    print(f"\n{'='*60}")
    print(f"VRP Complete: {metrics['total_scheduled']} scheduled, {metrics['total_unscheduled']} unscheduled")
    print(f"Total travel time: {metrics['total_travel_minutes']} minutes")
    print(f"Execution time: {metrics['execution_seconds']:.1f} seconds")
    print(f"{'='*60}")

    return {
        'assignments': all_assignments,
        'metrics': metrics
    }

def save_vrp_results(assignments, metrics):
    """Save VRP assignments to proposed_assignments table"""
    import uuid
    run_id = str(uuid.uuid4())
    
    supabase.table('vrp_runs').insert({
        'id': run_id,
        'inspection_ids': [a['inspection_id'] for a in assignments],
        'target_dates': list(set(a['date'] for a in assignments)),
        'status': 'COMPLETED',
        'num_inspections_scheduled': metrics['total_scheduled'],
        'total_travel_minutes': metrics['total_travel_minutes'],
        'execution_seconds': metrics['execution_seconds'],
        'requested_by': 'api',
        'triggered_by': 'api'
    }).execute()
    
    for assignment in assignments:
        supabase.table('proposed_assignments').insert({
            'vrp_run_id': run_id,
            **assignment
        }).execute()
    
    return run_id

if __name__ == "__main__":
    test_inspection_ids = [
        'a34b89e0-0539-43ed-82dc-05593190a8ab',
        'd807c526-548f-4ab7-a27b-421d383cdd27'
    ]
    test_dates = ['2025-10-15']
    result = run_vrp_for_inspections(test_inspection_ids, test_dates)
    print(json.dumps(result, indent=2))
    # Optionally persist:
    # run_id = save_vrp_results(result['assignments'], result['metrics'])
    # print("Saved VRP run:", run_id)
