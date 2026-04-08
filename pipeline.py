{\rtf1\ansi\ansicpg1252\cocoartf2868
\cocoatextscaling0\cocoaplatform0{\fonttbl\f0\fswiss\fcharset0 Helvetica;}
{\colortbl;\red255\green255\blue255;}
{\*\expandedcolortbl;;}
\margl1440\margr1440\vieww11520\viewh8400\viewkind0
\pard\tx720\tx1440\tx2160\tx2880\tx3600\tx4320\tx5040\tx5760\tx6480\tx7200\tx7920\tx8640\pardirnatural\partightenfactor0

\f0\fs24 \cf0 import pandas as pd\
import numpy as np\
import requests\
import re\
\
# =========================================================\
# FILE SETTINGS\
# =========================================================\
MASTER_FILE = "cases_master_cleaned_FINAL.csv"\
OUTPUT_FILE = "cases_master_cleaned_FINAL_UPDATED.csv"\
\
MAPBOX_TOKEN = "pk.eyJ1IjoiYXZlcnllam9obnMiLCJhIjoiY21uNmo3YnNiMDZrYTJwcTFwcHRzOG83NCJ9.aFx3CE9PzNOHSiDO7cEf2g"\
\
# =========================================================\
# MASTER SCHEMA\
# =========================================================\
MASTER_COLUMNS = [\
    "case_id",\
    "namus_id",\
    "identity_status",\
    "display_name",\
    "first_name",\
    "last_name",\
    "normalized_name",\
    "race",\
    "gender",\
    "age_at_event",\
    "case_type",\
    "victim_type",\
    "case_status",\
    "outcome",\
    "date_last_seen",\
    "date_body_found",\
    "year_last_seen",\
    "month_last_seen",\
    "raw_address",\
    "geocoded_address",\
    "latitude",\
    "longitude",\
    "geocode_precision",\
    "address_review_flag",\
    "coordinate_flag",\
    "city",\
    "state",\
    "location_found",\
    "estimated_pmi",\
    "circumstances_of_recovery",\
    "cause_of_death",\
    "case_summary",\
    "source_link",\
    "data_source_type",\
    "case_cluster_id",\
    "data_entry_stage",\
    "review_needed",\
    "usable_for_mapping",\
    "case_validity"\
]\
\
TEXT_LIKE_COLUMNS = [\
    "case_id",\
    "namus_id",\
    "identity_status",\
    "display_name",\
    "first_name",\
    "last_name",\
    "normalized_name",\
    "race",\
    "gender",\
    "case_type",\
    "victim_type",\
    "case_status",\
    "outcome",\
    "date_last_seen",\
    "date_body_found",\
    "raw_address",\
    "geocoded_address",\
    "geocode_precision",\
    "address_review_flag",\
    "coordinate_flag",\
    "city",\
    "state",\
    "location_found",\
    "estimated_pmi",\
    "circumstances_of_recovery",\
    "cause_of_death",\
    "case_summary",\
    "source_link",\
    "data_source_type",\
    "case_cluster_id",\
    "data_entry_stage",\
    "case_validity"\
]\
\
NUMERIC_LIKE_COLUMNS = [\
    "age_at_event",\
    "year_last_seen",\
    "month_last_seen",\
    "latitude",\
    "longitude",\
    "review_needed",\
    "usable_for_mapping"\
]\
\
# =========================================================\
# HELPERS\
# =========================================================\
def clean_text(x):\
    if pd.isna(x):\
        return pd.NA\
    x = str(x).strip()\
    x = re.sub(r"\\s+", " ", x)\
    return x if x else pd.NA\
\
def split_display_name(display_name):\
    if pd.isna(display_name):\
        return pd.NA, pd.NA\
    name = str(display_name).strip()\
    name = re.sub(r"\\s+", " ", name)\
    if not name:\
        return pd.NA, pd.NA\
    parts = name.split(" ")\
    if len(parts) == 1:\
        return parts[0], pd.NA\
    return parts[0], parts[-1]\
\
def normalize_name(first_name, last_name, display_name):\
    if pd.notna(first_name) or pd.notna(last_name):\
        full = f"\{'' if pd.isna(first_name) else first_name\} \{'' if pd.isna(last_name) else last_name\}".strip()\
    else:\
        full = "" if pd.isna(display_name) else str(display_name).strip()\
    full = re.sub(r"\\s+", " ", full).lower()\
    return full if full else pd.NA\
\
def standardize_case_type(x):\
    if pd.isna(x):\
        return pd.NA\
\
    raw = str(x).strip().lower()\
\
    homicide_terms = \{\
        "homicide", "murder", "murdered", "killed", "death investigation",\
        "deceased", "fatality", "fatal"\
    \}\
    missing_terms = \{\
        "missing", "missing person", "disappeared", "disappearance"\
    \}\
    unidentified_terms = \{\
        "unidentified", "unidentified person", "unknown", "jane doe", "doe"\
    \}\
\
    if raw in homicide_terms:\
        return "Homicide"\
    if raw in missing_terms:\
        return "Missing"\
    if raw in unidentified_terms:\
        return "Unidentified"\
\
    raw_title = str(x).strip().title()\
    if raw_title in \{"Homicide", "Missing", "Unidentified"\}:\
        return raw_title\
\
    return raw_title\
\
def infer_case_type(row):\
    existing = clean_text(row.get("case_type", pd.NA))\
    if pd.notna(existing):\
        standardized = standardize_case_type(existing)\
        if standardized in \{"Homicide", "Missing", "Unidentified"\}:\
            return standardized\
\
    outcome = clean_text(row.get("outcome", pd.NA))\
    victim_type = clean_text(row.get("victim_type", pd.NA))\
    identity_status = clean_text(row.get("identity_status", pd.NA))\
    cause_of_death = clean_text(row.get("cause_of_death", pd.NA))\
    case_status = clean_text(row.get("case_status", pd.NA))\
    summary = clean_text(row.get("case_summary", pd.NA))\
\
    text_blob = " ".join([\
        str(v).lower() for v in [\
            outcome, victim_type, identity_status, cause_of_death, case_status, summary\
        ] if pd.notna(v)\
    ])\
\
    if any(term in text_blob for term in ["homicide", "murder", "murdered", "killed", "fatal"]):\
        return "Homicide"\
    if any(term in text_blob for term in ["missing", "disappeared", "disappearance"]):\
        return "Missing"\
    if any(term in text_blob for term in ["unidentified", "unknown", "jane doe", "doe"]):\
        return "Unidentified"\
\
    return existing if pd.notna(existing) else pd.NA\
\
def enforce_schema_dtypes(df):\
    df = df.copy()\
    for col in TEXT_LIKE_COLUMNS:\
        if col in df.columns:\
            df[col] = df[col].astype("object")\
    for col in NUMERIC_LIKE_COLUMNS:\
        if col in df.columns:\
            df[col] = pd.to_numeric(df[col], errors="coerce")\
    return df\
\
def fill_name_parts_from_display(df):\
    """\
    Preserve display_name exactly as provided.\
    Only use display_name to fill first_name / last_name when those are blank.\
    """\
    df = df.copy()\
\
    df["display_name"] = df["display_name"].apply(clean_text)\
    df["first_name"] = df["first_name"].apply(clean_text)\
    df["last_name"] = df["last_name"].apply(clean_text)\
\
    split_names = df["display_name"].apply(split_display_name)\
    split_first = split_names.apply(lambda x: x[0])\
    split_last = split_names.apply(lambda x: x[1])\
\
    missing_first = df["first_name"].isna() & df["display_name"].notna()\
    missing_last = df["last_name"].isna() & df["display_name"].notna()\
\
    df.loc[missing_first, "first_name"] = split_first[missing_first]\
    df.loc[missing_last, "last_name"] = split_last[missing_last]\
\
    df["normalized_name"] = df.apply(\
        lambda row: normalize_name(row["first_name"], row["last_name"], row["display_name"]),\
        axis=1\
    )\
\
    return df\
\
def geocode_mapbox(address):\
    if pd.isna(address) or not str(address).strip():\
        return \{\
            "geocoded_address": pd.NA,\
            "latitude": pd.NA,\
            "longitude": pd.NA,\
            "geocode_precision": pd.NA,\
            "address_review_flag": "review",\
            "coordinate_flag": "missing"\
        \}\
\
    query = str(address).strip()\
    url = f"https://api.mapbox.com/geocoding/v5/mapbox.places/\{requests.utils.quote(query)\}.json"\
    params = \{\
        "access_token": MAPBOX_TOKEN,\
        "limit": 1,\
        "country": "us"\
    \}\
\
    try:\
        r = requests.get(url, params=params, timeout=20)\
        r.raise_for_status()\
        data = r.json()\
\
        if not data.get("features"):\
            return \{\
                "geocoded_address": pd.NA,\
                "latitude": pd.NA,\
                "longitude": pd.NA,\
                "geocode_precision": pd.NA,\
                "address_review_flag": "review",\
                "coordinate_flag": "missing"\
            \}\
\
        feat = data["features"][0]\
        lon, lat = feat["center"]\
\
        return \{\
            "geocoded_address": feat.get("place_name", pd.NA),\
            "latitude": lat,\
            "longitude": lon,\
            "geocode_precision": feat.get("place_type", [pd.NA])[0],\
            "address_review_flag": "ok",\
            "coordinate_flag": "ok"\
        \}\
\
    except Exception as e:\
        print(f"Geocoding failed for address: \{address\} | Error: \{e\}")\
        return \{\
            "geocoded_address": pd.NA,\
            "latitude": pd.NA,\
            "longitude": pd.NA,\
            "geocode_precision": pd.NA,\
            "address_review_flag": "review",\
            "coordinate_flag": "error"\
        \}\
\
def assign_case_ids_only_to_missing(df):\
    """\
    Preserve existing case_id values.\
    Only generate IDs for rows where case_id is missing.\
    """\
    df = df.copy()\
\
    df["case_id"] = df["case_id"].apply(clean_text)\
    df["case_type"] = df["case_type"].apply(standardize_case_type)\
\
    prefix_map = \{\
        "Missing": "MPD",\
        "Homicide": "MD",\
        "Unidentified": "UP"\
    \}\
\
    counters = \{"MPD": 0, "MD": 0, "UP": 0\}\
\
    existing_ids = df["case_id"].dropna().astype(str)\
    for cid in existing_ids:\
        m = re.match(r"^(MPD|MD|UP)-DC-(\\d+)$", cid)\
        if m:\
            prefix = m.group(1)\
            num = int(m.group(2))\
            counters[prefix] = max(counters[prefix], num)\
\
    missing_id_mask = df["case_id"].isna()\
\
    for idx in df.index[missing_id_mask]:\
        case_type = df.at[idx, "case_type"]\
        prefix = prefix_map.get(case_type)\
\
        if pd.isna(case_type) or prefix is None:\
            continue\
\
        counters[prefix] += 1\
        df.at[idx, "case_id"] = f"\{prefix\}-DC-\{str(counters[prefix]).zfill(5)\}"\
\
    return df\
\
def apply_flags(df):\
    df = df.copy()\
\
    df["usable_for_mapping"] = np.where(\
        df["latitude"].notna() & df["longitude"].notna(),\
        1,\
        0\
    )\
\
    df["review_needed"] = np.where(\
        df["display_name"].isna() |\
        df["raw_address"].isna() |\
        df["latitude"].isna() |\
        df["longitude"].isna(),\
        1,\
        0\
    )\
\
    df["case_validity"] = np.where(\
        df["usable_for_mapping"] == 1,\
        "valid_spatial_case",\
        "review_required"\
    )\
\
    df["address_review_flag"] = df["address_review_flag"].fillna("review")\
    df["coordinate_flag"] = df["coordinate_flag"].fillna("missing")\
\
    return df\
\
# =========================================================\
# MAIN PIPELINE\
# =========================================================\
def run_pipeline(new_file_name, replace_master=False):\
    print("Loading files...")\
\
    master = pd.read_csv(MASTER_FILE)\
    new = pd.read_csv(new_file_name)\
\
    for col in MASTER_COLUMNS:\
        if col not in master.columns:\
            master[col] = pd.NA\
        if col not in new.columns:\
            new[col] = pd.NA\
\
    master = master[MASTER_COLUMNS]\
    new = new[MASTER_COLUMNS]\
\
    master = enforce_schema_dtypes(master)\
    new = enforce_schema_dtypes(new)\
\
    print(f"Master rows before append: \{len(master)\}")\
    print(f"New intake rows: \{len(new)\}")\
\
    for col in new.columns:\
        if new[col].dtype == "object":\
            new[col] = new[col].apply(clean_text)\
\
    new = fill_name_parts_from_display(new)\
\
    new["case_type"] = new.apply(infer_case_type, axis=1)\
    new["case_type"] = new["case_type"].apply(standardize_case_type)\
\
    for col in ["age_at_event", "year_last_seen", "month_last_seen"]:\
        new[col] = pd.to_numeric(new[col], errors="coerce").astype("Int64")\
\
    for col in ["date_last_seen", "date_body_found"]:\
        parsed = pd.to_datetime(new[col], errors="coerce")\
        new[col] = parsed.dt.strftime("%Y-%m-%d")\
        new[col] = new[col].replace("NaT", pd.NA)\
\
    dt = pd.to_datetime(new["date_last_seen"], errors="coerce")\
    new["year_last_seen"] = new["year_last_seen"].fillna(dt.dt.year.astype("Int64"))\
    new["month_last_seen"] = new["month_last_seen"].fillna(dt.dt.month.astype("Int64"))\
\
    to_geocode_mask = (\
        new["raw_address"].notna() &\
        (new["raw_address"].astype(str).str.strip() != "") &\
        (new["latitude"].isna() | new["longitude"].isna())\
    )\
\
    print(f"Rows to geocode: \{to_geocode_mask.sum()\}")\
\
    if not MAPBOX_TOKEN or MAPBOX_TOKEN == "YOUR_MAPBOX_TOKEN_HERE":\
        print("Warning: add your Mapbox token before geocoding.")\
    elif to_geocode_mask.sum() > 0:\
        geocoded = new.loc[to_geocode_mask, "raw_address"].apply(geocode_mapbox).apply(pd.Series)\
\
        text_cols = [\
            "geocoded_address",\
            "geocode_precision",\
            "address_review_flag",\
            "coordinate_flag"\
        ]\
        numeric_cols = [\
            "latitude",\
            "longitude"\
        ]\
\
        for col in text_cols:\
            new[col] = new[col].astype("object")\
\
        for col in numeric_cols:\
            new[col] = pd.to_numeric(new[col], errors="coerce")\
\
        for col in text_cols + numeric_cols:\
            if col in geocoded.columns:\
                new.loc[to_geocode_mask, col] = geocoded[col].values\
\
    combined = pd.concat([master, new], ignore_index=True)\
\
    combined = enforce_schema_dtypes(combined)\
\
    for col in combined.columns:\
        if combined[col].dtype == "object":\
            combined[col] = combined[col].apply(clean_text)\
\
    combined = fill_name_parts_from_display(combined)\
    combined["case_type"] = combined.apply(infer_case_type, axis=1)\
    combined["case_type"] = combined["case_type"].apply(standardize_case_type)\
\
    for col in ["age_at_event", "year_last_seen", "month_last_seen"]:\
        combined[col] = pd.to_numeric(combined[col], errors="coerce").astype("Int64")\
\
    combined = combined.drop_duplicates(\
        subset=["display_name", "case_type", "date_last_seen", "raw_address", "source_link"],\
        keep="first"\
    ).reset_index(drop=True)\
\
    combined = assign_case_ids_only_to_missing(combined)\
    combined = apply_flags(combined)\
    combined = combined[MASTER_COLUMNS]\
\
    combined.to_csv(OUTPUT_FILE, index=False)\
\
    if replace_master:\
        combined.to_csv(MASTER_FILE, index=False)\
\
    print("\\nPipeline complete.")\
    print(f"Updated master saved as: \{OUTPUT_FILE\}")\
    print(f"Final row count: \{len(combined)\}")\
\
    print("\\nCase type counts:")\
    print(combined["case_type"].value_counts(dropna=False))\
\
    print("\\nRows needing review:")\
    print(combined["review_needed"].value_counts(dropna=False))\
\
    return combinedp}