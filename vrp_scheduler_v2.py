import os
import json
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

    # Get duration for each inspection (no .single(); handle 0/1+ rows safely)
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

    # Get available inspectors (filter lat IS NOT NULL in Python, not PostgREST)
    inspectors_result = supabase.table('inspectors') \
        .select('id, full_name, address, lat, lng') \
        .eq('is_active', True) \
        .execute()
    # Filter out null coordinates in Python
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

        # Coordinates
        coords_inspector_home = [(insp['home_lat'], insp['home_lng']) for insp in date_inspectors]

        coords_inspections: List[Tuple[float, float]] = []
        valid_inspections: List[Dict] = []
        for ins in date_inspections:
            if ins['lat'] and ins['lng']:
                coords_inspections.append((ins['lat'], ins['lng']))
                valid_inspections.append(ins)
            else:
                print(f"Skipping inspection at {ins['address']} - missing coordinates")
                metrics['total_unscheduled'] += 1
        if not valid_inspections:
            continue

        # Combine & deduplicate
        coords_all = coords_inspector_home + coords_inspections
        unique_coords: List[Tuple[float, float]] = []
        coord_to_idx: Dict[Tuple[float, float], int] = {}
        for c in coords_all:
            if c not in coord_to_idx:
                coord_to_idx[c] = len(unique_coords)
                unique_coords.append(c)
        n_real = len(unique_coords)

        print(f"Building travel matrix for {n_real} unique locations...")
        matrix = build_distance_matrix(unique_coords)

        # Dummy sink
        dummy = n_real
        for row in matrix:
            row.append(0)
        matrix.append([0] * (n_real + 1))
        n = n_real + 1

        # OR-Tools
        num_vehicles = len(date_inspectors)
        starts = [coord_to_idx[c] for c in coords_inspector_home]
        ends = [dummy] * num_vehicles

        mgr = pywrapcp.RoutingIndexManager(n, num_vehicles, starts, ends)
        routing = pywrapcp.RoutingModel(mgr)

        # Service durations
        durations = [0] * n_real
        for ins in valid_inspections:
            coord = (ins['lat'], ins['lng'])
            if coord in coord_to_idx:
                idx = coord_to_idx[coord]
                durations[idx] = ins['duration_minutes'] or 45
        durations.append(0)

        # Transit callback
        def time_callback(from_idx, to_idx):
            from_node = mgr.IndexToNode(from_idx)
            to_node = mgr.IndexToNode(to_idx)
            return matrix[from_node][to_node] + durations[from_node]

        transit_callback_index = routing.RegisterTransitCallback(time_callback)
        routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

        # ======= Time windows (replaced logic) =======
        print("\nInspector availability windows:")
        time_windows: List[Tuple[int, int]] = []
        for insp in date_inspectors:
            start_time_obj = datetime.strptime(insp['start_time_local'], '%H:%M:%S').time()
            end_time_obj = datetime.strptime(insp['end_time_local'], '%H:%M:%S').time()
            start_min = start_time_obj.hour * 60 + start_time_obj.minute
            end_min = end_time_obj.hour * 60 + end_time_obj.minute
            time_windows.append((start_min, end_min))
            print(f"  {insp['full_name']}: {insp['start_time_local']} - {insp['end_time_local']} ({start_min}-{end_min} min from midnight)")

        max_duration = max(end - start for start, end in time_windows)

        routing.AddDimension(
            transit_callback_index,
            max_duration,          # slack
            max_duration + 480,    # max time per vehicle (buffer)
            False,
            'Time'
        )
        time_dimension = routing.GetDimensionOrDie('Time')

        for vehicle_id, (start_min, end_min) in enumerate(time_windows):
            start_index = routing.Start(vehicle_id)
            end_index = routing.End(vehicle_id)
            time_dimension.CumulVar(start_index).SetRange(start_min, start_min)
            time_dimension.CumulVar(end_index).SetRange(start_min, end_min)
        # ======= End time windows =======

        # Skill constraints
        print("\nApplying skill constraints...")
        for ins in valid_inspections:
            coord = (ins['lat'], ins['lng'])
            if coord not in coord_to_idx:
                continue
            node = coord_to_idx[coord]
            node_index = mgr.NodeToIndex(node)
            allowed_vehicles: List[int] = []
            for vehicle_id, insp in enumerate(date_inspectors):
                if ins['inspection_type'] in insp['can_do_types']:
                    allowed_vehicles.append(vehicle_id)
            if allowed_vehicles:
                routing.SetAllowedVehiclesForIndex(allowed_vehicles, node_index)
                print(f"  {ins['inspection_type']} at {ins['address'][:30]}... → inspectors {allowed_vehicles}")
            else:
                print(f"  WARNING: No qualified inspector for {ins['inspection_type']}")
                metrics['total_unscheduled'] += 1

        # Solve
        search_parameters = pywrapcp.DefaultRoutingSearchParameters()
        search_parameters.first_solution_strategy = routing_enums_pb2.FirstSolutionStrategy.PATH_CHEAPEST_ARC
        search_parameters.local_search_metaheuristic = routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
        search_parameters.time_limit.seconds = 60

        print(f"\nSolving VRP with {num_vehicles} inspectors and {len(valid_inspections)} inspections...")
        solution = routing.SolveWithParameters(search_parameters)
        if not solution:
            print("No complete solution found")
            metrics['total_unscheduled'] += len(valid_inspections)
            continue

        print("✓ Solution found!")

        # ======= EXTRACT SOLUTION (calculate from midnight) =======
        tz = pytz.timezone('Europe/Copenhagen')
        base_date = datetime.strptime(inspection_date, '%Y-%m-%d').date()
        day_midnight = tz.localize(datetime.combine(base_date, datetime.min.time()))

        for vehicle_id in range(num_vehicles):
            insp = date_inspectors[vehicle_id]

            index = routing.Start(vehicle_id)
            route_sequence = 0

            while not routing.IsEnd(index):
                node = mgr.IndexToNode(index)

                # Skip depot/dummy nodes
                if node >= n_real or node in starts:
                    index = solution.Value(routing.NextVar(index))
                    continue

                node_coord = unique_coords[node]
                matching_inspection = None
                for ins in valid_inspections:
                    if (ins['lat'], ins['lng']) == node_coord:
                        matching_inspection = ins
                        break
                if not matching_inspection:
                    index = solution.Value(routing.NextVar(index))
                    continue

                # Minutes from midnight
                time_var = time_dimension.CumulVar(index)
                time_min = solution.Value(time_var)

                # Calculate from midnight
                start_dt = day_midnight + timedelta(minutes=time_min)
                start_dt = round_to_nearest_5_min(start_dt)

                duration = matching_inspection['duration_minutes'] or 45
                end_dt = start_dt + timedelta(minutes=duration)

                # Travel time (best-effort calc)
                travel_time = 0
                if route_sequence > 0:
                    prev_time = solution.Value(time_dimension.CumulVar(index))
                    travel_time = time_min - prev_time - durations[mgr.IndexToNode(index)]

                route_sequence += 1

                all_assignments.append({
                    'inspection_id': matching_inspection['id'],
                    'inspector_id': insp['inspector_id'],
                    'date': inspection_date,
                    'start_time': start_dt.time().isoformat(),
                    'end_time': end_dt.time().isoformat(),
                    'sequence': route_sequence,
                    'travel_mins': max(0, int(travel_time))
                })

                metrics['total_scheduled'] += 1
                metrics['total_travel_minutes'] += max(0, int(travel_time))

                print(f"  {insp['full_name']}: {matching_inspection['inspection_type']} at {start_dt.strftime('%H:%M')}")

                index = solution.Value(routing.NextVar(index))
        # ======= END EXTRACT SOLUTION =======

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

# After the return statement, add function to save results
def save_vrp_results(assignments, metrics):
    """Save VRP assignments to proposed_assignments table"""
    import uuid
    run_id = str(uuid.uuid4())
    
    # Save run metadata
    supabase.table('vrp_runs').insert({
        'id': run_id,
        'inspection_ids': [a['inspection_id'] for a in assignments],
        'target_dates': list(set(a['date'] for a in assignments)),
        'status': 'COMPLETED',
        'num_inspections_scheduled': metrics['total_scheduled'],
        'total_travel_minutes': metrics['total_travel_minutes'],
        'execution_seconds': metrics['execution_seconds']
    }).execute()
    
    # Save proposed assignments
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