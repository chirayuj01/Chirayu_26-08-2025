import pandas as pd
import pytz
import uuid
from datetime import datetime, timedelta, time
from db import get_connection

DEFAULT_TZ = "America/Chicago"

def _parse_time_string(s):
    """Parse a time string like '09:00:00' or '09:00' into a datetime.time"""
    if pd.isna(s):
        return None
    try:
        return pd.to_datetime(s).time()
    except Exception:
        parts = str(s).split(":")
        h = int(parts[0])
        m = int(parts[1]) if len(parts) > 1 else 0
        return time(h, m)

def _get_business_map(bh_df):
    """
    Returns dict: store_id -> { day_of_week (0=Mon)-> [ (start_time, end_time), ... ] }
    If a store has no business hours rows, it will be left out (caller can treat as 24x7).
    """
    biz = {}
    if bh_df is None or bh_df.empty:
        return biz

    day_col = None
    for c in ["dayOfWeek", "day_of_week", "dayOfweek", "day"]:
        if c in bh_df.columns:
            day_col = c
            break
    if day_col is None:
        raise ValueError("business_hours table missing dayOfWeek/day_of_week column")

    for _, r in bh_df.iterrows():
        sid = r["store_id"]
        dow = int(r[day_col])
        s = _parse_time_string(r.get("start_time_local", r.get("start_time", None)))
        e = _parse_time_string(r.get("end_time_local", r.get("end_time", None)))
        if s is None or e is None:
            continue
        biz.setdefault(sid, {}).setdefault(dow, []).append((s, e))
    return biz

def _utc_interval_overlap(a_start_utc, a_end_utc, b_start_utc, b_end_utc):
    """
    Return overlap duration in seconds between two UTC-aware intervals [a_start,a_end) and [b_start,b_end).
    All inputs must be timezone-aware (tzinfo=UTC) or pandas.Timestamp with tzinfo.
    """
    start = max(a_start_utc, b_start_utc)
    end = min(a_end_utc, b_end_utc)
    if end <= start:
        return 0.0
    return (end - start).total_seconds()

def compute_report():
    """
    Produces CSV report with columns:
      store_id,
      uptime_last_hour(in minutes),
      uptime_last_day(in hours),
      uptime_last_week(in hours),
      downtime_last_hour(in minutes),
      downtime_last_day(in hours),
      downtime_last_week(in hours)

    Logic:
      - Use max timestamp in store_status as 'now'.
      - For each store:
          - Determine tz (from store_timezone table or default).
          - Build business-hour windows per day within each period.
          - For each business-hour window, compute status over time by:
              - Finding polls for the store (full history) and using forward-fill
                semantics (status at poll at t applies from t until next poll).
              - If no prior poll exists for a window start, assume DEFAULT_STATUS = 'inactive'.
          - Sum overlaps of 'active' and 'inactive' segments inside business windows.
      - Output values as required (minutes for last hour, hours for day/week).
    """
    conn = get_connection()
    
    status_df = pd.read_sql("SELECT * FROM store_status", conn, parse_dates=["timestamp_utc"])
    bh_df = pd.read_sql("SELECT * FROM business_hours", conn) if "business_hours" in pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn).values else pd.DataFrame()
    tz_df = pd.read_sql("SELECT * FROM store_timezone", conn) if "store_timezone" in pd.read_sql("SELECT name FROM sqlite_master WHERE type='table'", conn).values else pd.DataFrame()

    if status_df.empty:
        raise ValueError("No status data available")

    
    status_df["timestamp_utc"] = pd.to_datetime(status_df["timestamp_utc"], utc=True)

    
    bh_map = _get_business_map(bh_df)

    
    tz_map = {}
    if not tz_df.empty:
        for _, r in tz_df.iterrows():
            tz_map[r["store_id"]] = r.get("timezone_str") or DEFAULT_TZ

    
    stores = set(status_df["store_id"].unique()) | set(bh_map.keys()) | set(tz_map.keys())

    now = status_df["timestamp_utc"].max()  
    periods = {
        "last_hour": timedelta(hours=1),
        "last_day": timedelta(days=1),
        "last_week": timedelta(days=7),
    }

    rows = []

    
    status_by_store = {sid: grp.sort_values("timestamp_utc").reset_index(drop=True)
                       for sid, grp in status_df.groupby("store_id")}

    
    for store in sorted(stores):
        g = status_by_store.get(store, pd.DataFrame(columns=status_df.columns))
        
        tz_name = tz_map.get(store, DEFAULT_TZ)
        try:
            store_tz = pytz.timezone(tz_name)
        except Exception:
            store_tz = pytz.timezone(DEFAULT_TZ)

        
        if store not in bh_map:
            bh_map_store = {dow: [(time(0,0,0), time(23,59,59))] for dow in range(7)}
        else:
            bh_map_store = bh_map[store]

        stats = {}
        
        poll_ts = list(g["timestamp_utc"]) if not g.empty else []
        poll_status = list(g["status"]) if not g.empty else []

        
        def _last_poll_index_before(ts_utc):
            
            if not poll_ts:
                return None
            
            idx = g["timestamp_utc"].searchsorted(ts_utc) - 1
            if idx >= 0:
                return int(idx)
            return None

        
        for label, delta in periods.items():
            window_start = now - delta
            window_end = now

            total_active_seconds = 0.0
            total_seconds = 0.0  

            
            day_start_date = window_start.date()
            day_end_date = window_end.date()

            for single_date in pd.date_range(start=day_start_date, end=day_end_date, freq="D"):
                dow = single_date.weekday()  
                
                bh_list = bh_map_store.get(dow, [])
                if not bh_list:
                    continue
                for (bh_start_time, bh_end_time) in bh_list:
                    
                    bh_start_local = store_tz.localize(datetime.combine(single_date, bh_start_time))
                    bh_end_local = store_tz.localize(datetime.combine(single_date, bh_end_time))

                    
                    if bh_end_local <= bh_start_local:
                        bh_end_local = bh_end_local + timedelta(days=1)

                    
                    bh_start_utc = bh_start_local.astimezone(pytz.UTC)
                    bh_end_utc = bh_end_local.astimezone(pytz.UTC)

                    
                    seg_start = max(bh_start_utc, window_start)
                    seg_end = min(bh_end_utc, window_end)
                    if seg_start >= seg_end:
                        continue

                    seg_total_seconds = (seg_end - seg_start).total_seconds()
                    total_seconds += seg_total_seconds

                    if not g.empty:
                        left_idx = g["timestamp_utc"].searchsorted(seg_start)
                        right_idx = g["timestamp_utc"].searchsorted(seg_end)
                       
                        inside_idx = list(range(int(left_idx), int(right_idx)))
                    else:
                        inside_idx = []

                   
                    prev_idx = _last_poll_index_before(seg_start)
                    if prev_idx is not None:
                        current_status = str(g.iloc[prev_idx]["status"])
                    else:
                        
                        current_status = "inactive"

                    
                    cut_points = [seg_start]
                    for idx in inside_idx:
                        cut_points.append(g.iloc[idx]["timestamp_utc"])
                    cut_points.append(seg_end)

                    
                    for i in range(len(cut_points)-1):
                        left = cut_points[i]
                        right = cut_points[i+1]
                        if right <= left:
                            continue
                        duration = (right - left).total_seconds()
                        
                        if i > 0:
                            poll_idx = inside_idx[i-1]
                            current_status = str(g.iloc[poll_idx]["status"])
                        
                        if current_status == "active":
                            total_active_seconds += duration
                        

            
            if label == "last_hour":
                
                up_mins = round((total_active_seconds / 60.0), 2)
                down_mins = round(((total_seconds - total_active_seconds) / 60.0), 2)
                stats[f"uptime_{label}(mins)"] = up_mins
                stats[f"downtime_{label}(mins)"] = down_mins
            else:
                up_hours = round((total_active_seconds / 3600.0), 2)
                down_hours = round(((total_seconds - total_active_seconds) / 3600.0), 2)
                stats[f"uptime_{label}(hours)"] = up_hours
                stats[f"downtime_{label}(hours)"] = down_hours

        rows.append({"store_id": store, **stats})

    out = pd.DataFrame(rows, columns=[
        "store_id",
        "uptime_last_hour(mins)","downtime_last_hour(mins)",
        "uptime_last_day(hours)","downtime_last_day(hours)",
        "uptime_last_week(hours)","downtime_last_week(hours)"
    ])

    path = f"report_{uuid.uuid4().hex}.csv"
    out.to_csv(path, index=False)
    conn.close()
    return path
