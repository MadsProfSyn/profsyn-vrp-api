import os
import json
from collections import defaultdict
from ortools.constraint_solver import routing_enums_pb2
from ortools.constraint_solver import pywrapcp
from datetime import datetime, timedelta
from typing import Tuple, Optional, List, Dict
import pytz
from supabase import create_client, Client
from dotenv import load_dotenv

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_KEY")
supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)

def get_travel_time_from_cache(from_lat: float, from_lng: float, 
                               to_lat: float, to_lng: float) -> float:
    """Get cached travel time or return estimate"""
    key = f"{from_lng},{from_lat}->{to_lng},{to_lat}"
    result = supabase.table('mapbox_travel_cache').select('minutes').eq('key', key).execute()
    if result.data:
        return max(5.0, result.data[0]['minutes'])

    # Haversine fallback if not cached
    from math import radians, cos, sin, asin, sqrt
    lon1, lat1, lon2, lat2 = map(radians, [from_lng, from_lat, to_lng, to_lat])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6371 * c
    # Assume 40 km/h average speed in Copenhagen
    minutes = (km / 40) * 60
    return max(5.0, minutes)

def build_distance_matrix(coords_list: List[Tuple[float, float]]) -> List[List[int]]:
    """Build travel time matrix using cached data"""
    size = len(coords_list)
    matrix = [[0 for _ in range(size)] for _ in range(size)]
    for i in range(size):
        for j in range(size):
            if i == j:
                continue
            lat1, lng1 = coords_list[i]
            lat2, lng2 = coords_list[j]
            travel_time = get_travel_time_from_cache(lat1, lng1, lat2, lng2)
            matrix[i][j] = max(5, int(travel_time))
    return matrix

def round_to_nearest_5_min(dt: datetime) -> datetime:
    """Round datetime to nearest 5 minutes"""
    discard = timedelta(minutes=dt.minute % 5, seconds=dt.second, microseconds=dt.microsecond)
    dt += timedelta(minutes=5) - discard if discard else timedelta()
    return dt.replace(second=0, microsecond=0)

def run_vrp_for_inspections(inspection_ids: List[str], target_dates: List[str]) -> Dict:
    """Main VRP function adapted for new schema"""
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
        coords: List[Tuple[float, float]] = []
        for ins in date_inspections:
            if ins['lat'] and ins['lng']:
                inspection_nodes.append(ins)                  # keep each inspection as its own node
                coords.append((ins['lat'], ins['lng']))       # 1:1 with nodes
            else:
                print(f"Skipping inspection at {ins['address']} - missing coordinates")
                metrics['total_unscheduled'] += 1

        if not inspection_nodes:
            continue

        n_real = len(inspection_nodes)
        print(f"\nNode count (no dedup): {n_real} (should equal number of valid inspections for this date)")

        # --- Eligibility diagnostics ---
        eligible_counts = defaultdict(int)
        for ins in inspection_nodes:
            for insp in date_inspectors:
                if ins['inspection_type'] in insp['can_do_types']:
                    eligible_counts[insp['full_name']] += 1
        print("\nEligible visit counts per inspector:")
        for insp in date_inspectors:
            print(f"  {insp['full_name']}: {eligible_counts.get(insp['full_name'], 0)} eligible")

        print("Building travel matrix...")
        matrix = build_distance_matrix(coords)

        # Add dummy depot (no actual location, just for OR-Tools structure)
        dummy = n_real
        for row in matrix:
            row.append(0)
        matrix.append([0] * (n_real + 1))
        n = n_real + 1

        # OR-Tools - all inspectors start/end at dummy depot
        num_vehicles = len(date_inspectors)
        starts = [dummy] * num_vehicles
        ends = [dummy] * num_vehicles

        mgr = pywrapcp.RoutingIndexManager(n, num_vehicles, starts, ends)
        routing = pywrapcp.RoutingModel(mgr)

        # Service durations aligned 1:1 with inspection_nodes
        durations = [0] * n_real
        for idx, ins in enumerate(inspection_nodes):
            durations[idx] = ins.get('duration_minutes') or 45
        durations.append(0)  # dummy

        # Transit callback
        def time_callback(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            to_node = mgr.IndexToNode(to_idx)
            return matrix[from_node][to_node] + durations[from_node]

        transit_callback_index = routing.RegisterTransitCallback(time_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # Time windows (soft overtime allowed on vehicle end)
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
            transit_callback_index,
            0,                # no extra waiting slack
            day_horizon,      # max cumul
            False,
            'Time'
        )
        time_dimension = routing.GetDimensionOrDie('Time')

        # Allow soft overtime beyond end_min with per-minute penalty
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

        # ---------- Optional load-balancing on service minutes ----------
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
        LOAD_CAP = 300          # ~5 hours of service
        LOAD_PENALTY = 50       # tune up/down to push balancing
        for v in range(num_vehicles):
            end_idx = routing.End(v)
            svc_dim.SetCumulVarSoftUpperBound(end_idx, LOAD_CAP, LOAD_PENALTY)
        # ----------------------------------------------------------------

        # Skill constraints (use node index directly)
        print("\nApplying skill constraints...")
        for idx, ins in enumerate(inspection_nodes):
            node_index = mgr.NodeToIndex(idx)
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

        # BIG penalties for dropping any inspection node
        BIG_DROP_PENALTY = 1_000_000
        for node in range(n_real):
            routing.AddDisjunction([mgr.NodeToIndex(node)], BIG_DROP_PENALTY)

        # Solve with increased timeout
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 300  # give it more time

        print(f"\nSolving VRP with {num_vehicles} inspectors and {len(inspection_nodes)} nodes (1 per inspection)...")
        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            print("No complete solution found")
            metrics['total_unscheduled'] += len(inspection_nodes)
            continue

        print("✓ Solution found!")

        # --- Dropped node diagnostics ---
        dropped = []
        for node in range(n_real):
            node_index = mgr.NodeToIndex(node)
            if solution.Value(routing.NextVar(node_index)) == node_index:
                dropped.append(inspection_nodes[node])

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

                # Skip dummy depot
                if node == dummy:
                    index = solution.Value(routing.NextVar(index))
                    continue

                ins = inspection_nodes[node]

                # Minutes from midnight
                time_var = time_dimension.CumulVar(index)
                time_min = solution.Value(time_var)

                start_dt = day_midnight + timedelta(minutes=time_min)
                start_dt = round_to_nearest_5_min(start_dt)

                duration = ins.get('duration_minutes') or 45
                end_dt = start_dt + timedelta(minutes=duration)

                # Travel time between inspections (matrix minutes)
                travel_time = 0
                if route_sequence > 0 and prev_node is not None:
                    travel_time = matrix[prev_node][node]

                route_sequence += 1
                prev_node = node

                # --- PRINT WITH ADDRESS ---
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
