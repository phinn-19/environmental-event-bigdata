"""Foundation-only config + fixture validators. No Spark or API access."""
import datetime as dt
import json
import math
from pathlib import Path

import yaml


class ContractError(ValueError):
    pass


def read_json(path):
    return json.loads(Path(path).read_text(encoding='utf-8'), parse_constant=lambda token: (_ for _ in ()).throw(ContractError(f'Non-finite JSON: {token}')))


def is_utc_z(value):
    if not isinstance(value, str) or not value.endswith('Z'):
        return False
    try:
        parsed = dt.datetime.fromisoformat(value.replace('Z', '+00:00'))
        return parsed.tzinfo == dt.timezone.utc
    except ValueError:
        return False


def timestamp(value):
    if not is_utc_z(value):
        raise ContractError(f'Expected ISO-8601 UTC Z, got {value!r}')
    return dt.datetime.fromisoformat(value.replace('Z', '+00:00'))


def _ensure(condition, error):
    if not condition:
        raise ContractError(error)


def validate_config(config_file):
    path = Path(config_file)
    config = yaml.safe_load(path.read_text(encoding='utf-8'))
    _ensure(isinstance(config, dict), 'Config must be a YAML object')
    proj = config.get('project', {})
    _ensure(proj.get('timezone') == 'UTC', 'project.timezone must be UTC')
    _ensure(proj.get('crs') == 'EPSG:4326', 'project.crs must be EPSG:4326')
    _ensure(isinstance(proj.get('data_root'), str) and proj['data_root'], 'project.data_root required')
    _ensure(config.get('processing', {}).get('timezone') == 'UTC', 'Spark timezone must be UTC')
    regions = config.get('regions', {})
    _ensure(isinstance(regions, dict) and {'VN_BBOX','HANOI'} <= set(regions), 'VN_BBOX/HANOI required')
    for region, bbox in regions.items():
        _ensure(all(key in bbox and isinstance(bbox[key],(int,float)) and not isinstance(bbox[key],bool) for key in ('north','south','west','east')), f'{region}: bbox missing values')
        _ensure(-90 <= bbox['south'] < bbox['north'] <= 90, f'{region}: invalid latitude')
        _ensure(-180 <= bbox['west'] < bbox['east'] <= 180, f'{region}: invalid longitude')
    integration = config.get('integration', {})
    start, end = integration.get('start_utc'), integration.get('end_utc')
    _ensure((start is None) == (end is None), 'integration start/end must both be null or both set')
    if start is not None:
        _ensure(timestamp(start) < timestamp(end), 'integration window invalid')
    _ensure(integration.get('region_id') in regions, 'integration region_id must be known')
    for key in ('raw','silver','curated','events','demo'):
        value = config.get('storage',{}).get(key)
        _ensure(isinstance(value,str) and value.startswith('data/'), f'storage.{key} must be data-relative')
    # v1.1 contract defaults: validate interface parameters without implementing algorithm.
    alg = config.get('algorithms', {})
    aq = alg.get('air_quality', {})
    baseline = aq.get('baseline', {})
    anomaly = aq.get('anomaly', {})
    fire = alg.get('fire', {})
    _ensure(baseline.get('method') == 'same_utc_hour_median_30d_v1', 'Unsupported AQ baseline method (v1.1)')
    for name in ('lookback_days', 'min_valid_samples'):
        _ensure(type(baseline.get(name)) is int and baseline[name] > 0, f'baseline.{name}: expected positive integer')
    _ensure(baseline['min_valid_samples'] <= baseline['lookback_days'], 'baseline min_valid_samples cannot exceed lookback_days for same-hour daily series')
    _ensure(baseline.get('exclude_flagged') is True, 'MVP baseline must exclude flagged rows')
    for name in ('min_absolute_increase_ugm3','min_ratio'):
        value = anomaly.get(name)
        _ensure(type(value) in (int,float) and math.isfinite(value) and value > 0, f'anomaly.{name}: expected positive finite number')
    _ensure(type(anomaly.get('min_consecutive_hours')) is int and anomaly['min_consecutive_hours'] >= 2, 'anomaly.min_consecutive_hours must be integer >= 2')
    _ensure(fire.get('algorithm') == 'st_dbscan_v1', 'Unsupported fire algorithm (v1.1)')
    for name in ('spatial_radius_km','temporal_radius_hours'):
        value = fire.get(name)
        _ensure(type(value) in (int,float) and math.isfinite(value) and value > 0, f'fire.{name}: expected positive finite number')
    _ensure(type(fire.get('min_samples')) is int and fire['min_samples'] >= 2, 'fire.min_samples must be integer >= 2')
    conf = fire.get('accepted_confidence')
    _ensure(isinstance(conf,list) and set(conf) == {'n','h'}, 'fire.accepted_confidence must be [n,h] in v1.1')
    _ensure(fire.get('historical_preference') == config.get('ingestion',{}).get('firms',{}).get('historical_product_preference'), 'FIRMS canonical product settings disagree')
    _ensure(fire.get('severity') == 'undetermined', 'MVP severity should be undetermined until evaluated')
    _ensure(not any(x in path.read_text(encoding='utf-8').lower() for x in ['your_personal_access_token','firms_map_key:','openaq_api_key:']), 'Public config may not store API keys')
    return config


def validate_table_rows(table_name, rows, registry):
    tables = registry.get('tables', {})
    _ensure(table_name in tables, f'Unknown table {table_name}')
    _ensure(isinstance(rows,list), f'{table_name}: expected JSON array')
    fields = tables[table_name]['fields']
    unique_keys = tables[table_name]['key']
    seen=set()
    for index,row in enumerate(rows):
        prefix=f'{table_name}[{index}]'
        _ensure(isinstance(row,dict), f'{prefix}: object required')
        _ensure(set(row) == set(fields), f'{prefix}: field mismatch, missing={set(fields)-set(row)}, extra={set(row)-set(fields)}')
        for field,type_hint in fields.items():
            nullable=type_hint.endswith('?')
            type_name=type_hint.removesuffix('?')
            value=row[field]
            if value is None:
                _ensure(nullable, f'{prefix}.{field}: null not allowed')
                continue
            if type_name in ('string','utc'):
                _ensure(isinstance(value,str), f'{prefix}.{field}: expected string')
                if type_name=='utc': _ensure(is_utc_z(value), f'{prefix}.{field}: expected UTC Z')
            elif type_name=='double':
                _ensure(type(value) in (float,int) and math.isfinite(value), f'{prefix}.{field}: finite number required')
            elif type_name=='int':
                _ensure(type(value) is int, f'{prefix}.{field}: int required')
            elif type_name=='boolean':
                _ensure(type(value) is bool, f'{prefix}.{field}: bool required')
            else:
                raise ContractError(f'Unsupported type hint {type_hint}')
        if 'latitude' in row: _ensure(-90<=row['latitude']<=90, f'{prefix}: latitude out of bounds')
        if 'longitude' in row: _ensure(-180<=row['longitude']<=180, f'{prefix}: longitude out of bounds')
        if 'period_end_utc' in row:
            _ensure(timestamp(row['period_end_utc']) > timestamp(row['timestamp_utc']), f'{prefix}: invalid period')
        key=tuple(row[key] for key in unique_keys)
        _ensure(key not in seen, f'{prefix}: duplicate primary key {key}')
        seen.add(key)
    return len(rows)


def validate_events(events):
    _ensure(isinstance(events,list), 'Events must be a JSON array; [] is valid')
    required={'event_id','event_type','region_id','start_time_utc','end_time_utc','centroid_latitude','centroid_longitude','severity','metrics','weather_context','evidence_ids','algorithm_version'}
    unique_ids=set()
    for i,event in enumerate(events):
        p=f'events[{i}]'
        _ensure(isinstance(event,dict) and set(event)==required, f'{p}: keys differ from contract')
        _ensure(isinstance(event['event_id'],str) and bool(event['event_id']), f'{p}: bad id')
        _ensure(event['event_id'] not in unique_ids, f'{p}: duplicate id')
        unique_ids.add(event['event_id'])
        _ensure(event['event_type'] in ('AIR_QUALITY_ANOMALY','FIRE_HOTSPOT_CLUSTER'), f'{p}: bad type')
        _ensure(isinstance(event['region_id'],str) and bool(event['region_id']), f'{p}: region required')
        _ensure(timestamp(event['start_time_utc']) < timestamp(event['end_time_utc']), f'{p}: invalid window')
        for field,low,high in [('centroid_latitude',-90,90),('centroid_longitude',-180,180)]:
            value=event[field]
            _ensure(type(value) in (int,float) and math.isfinite(value) and low<=value<=high, f'{p}: {field} invalid')
        _ensure(event['severity'] in ('low','medium','high','undetermined'), f'{p}: bad severity')
        _ensure(isinstance(event['algorithm_version'],str) and bool(event['algorithm_version']), f'{p}: algorithm_version required')
        _ensure(isinstance(event['evidence_ids'],list) and all(isinstance(x,str) and x for x in event['evidence_ids']),f'{p}: bad evidence_ids')
        met=event['metrics']
        _ensure(isinstance(met,dict), f'{p}: metrics must be object')
        if event['event_type']=='AIR_QUALITY_ANOMALY':
            keys={'sensor_ids','parameter','peak_ugm3','mean_ugm3','baseline_ugm3','duration_hours'}
            _ensure(set(met)==keys, f'{p}: AQ metrics keys')
            _ensure(isinstance(met['sensor_ids'],list) and met['sensor_ids'] and all(isinstance(x,str) for x in met['sensor_ids']),f'{p}: sensor_ids')
            _ensure(met['parameter']=='pm25',f'{p}: parameter')
            for x in ('peak_ugm3','mean_ugm3','baseline_ugm3','duration_hours'):
                _ensure(type(met[x]) in (int,float) and math.isfinite(met[x]), f'{p}: {x} nonfinite')
            _ensure(met['duration_hours']>0, f'{p}: duration_hours')
        else:
            keys={'hotspot_count','max_frp_mw','sum_frp_mw','satellites'}
            _ensure(set(met)==keys, f'{p}: fire metrics keys')
            _ensure(type(met['hotspot_count']) is int and met['hotspot_count']>=2,f'{p}: minimum 2 hotspots')
            for x in ('max_frp_mw','sum_frp_mw'):
                v=met[x]
                _ensure(v is None or (type(v) in (int,float) and math.isfinite(v)),f'{p}: {x} invalid')
            _ensure(isinstance(met['satellites'],list) and all(isinstance(x,str) for x in met['satellites']), f'{p}: bad satellites')
        weather=event['weather_context']
        if weather is not None:
            _ensure(isinstance(weather,dict) and set(weather)=={'temperature_c','wind_u_ms','wind_v_ms','precipitation_mm_1h','context_method'},f'{p}: bad weather keys')
            for x in ('temperature_c','wind_u_ms','wind_v_ms','precipitation_mm_1h'):
                val=weather[x]
                _ensure(val is None or (type(val) in (int,float) and math.isfinite(val)), f'{p}: nonfinite weather')
            _ensure(isinstance(weather['context_method'],str) and bool(weather['context_method']),f'{p}: context_method')
    return len(events)


def validate_fixture_set(root):
    root=Path(root)
    registry=read_json(root/'contracts/schemas/table_contracts.json')
    counts={}
    for group in ('silver','curated'):
        for path in sorted((root/'fixtures'/group).glob('*.json')):
            counts[path.stem] = validate_table_rows(path.stem, read_json(path), registry)
    _ensure(set(counts)==set(registry['tables']), f'Missing table fixtures: {set(registry["tables"])-set(counts)}')
    counts['events'] = validate_events(read_json(root/'fixtures/events/events_mock.json'))
    _ensure((root/'contracts/schemas/events.schema.json').exists(), 'Missing event schema')
    _ensure((root/'contracts/schemas/manifest.schema.json').exists(), 'Missing manifest schema')
    return counts
