import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta, time, timezone
import sys
import os
import json
import time as pytime
import threading
import signal
from streamlit_autorefresh import st_autorefresh
from streamlit.runtime import Runtime
from cryptography.fernet import Fernet

def _monitor_sessions():
    """Shuts down the process when no active sessions are left."""
    pytime.sleep(10) # Initial grace period for app to start
    while True:
        try:
            runtime = Runtime.instance()
            session_count = len(runtime._session_mgr.list_active_sessions())
            if session_count == 0:
                # Final grace period for refreshes
                pytime.sleep(10)
                session_count = len(runtime._session_mgr.list_active_sessions())
                if session_count == 0:
                    os._exit(0)
        except Exception:
            pass
        pytime.sleep(5)

# Initialize Session Monitor
if not any(t.name == "SessionMonitor" for t in threading.enumerate()):
    monitor_thread = threading.Thread(target=_monitor_sessions, name="SessionMonitor", daemon=True)
    monitor_thread.start()


# Add src to path to allow imports if running directly
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

import src.lang
import importlib
importlib.reload(src.lang)
from src.lang import get_text, STRINGS, DEFAULT_METRICS
from src.auth import authenticate
from src.api import GenesysAPI
from src.processor import process_analytics_response, to_excel, fill_interval_gaps, process_observations, process_daily_stats

# Streamlit config
st.set_page_config(page_title="Genesys Cloud Reporting", layout="wide")

# Custom CSS: Hide password visibility toggle & prevent refresh fade
st.markdown("""
<style>
    /* Modern Dashboard Design (V4) - SaaS Style */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    
    html, body, [data-testid="stAppViewContainer"] {
        font-family: 'Inter', sans-serif !important;
        background-color: #ffffff !important;
    }

    /* Group Card - Pure White with Soft Border */
    [data-testid="stVerticalBlockBorderWrapper"] {
        background-color: #ffffff !important;
        border: 1px solid #eef2f6 !important;
        border-radius: 12px !important;
        padding: 1.5rem !important;
        margin-bottom: 1.5rem !important;
        box-shadow: 0 1px 3px rgba(0,0,0,0.02) !important;
    }

    /* Metric Boxes - Light Grey Background for Contrast */
    [data-testid="stMetricContainer"] {
        background-color: #f8fafb !important;
        border: 1px solid #f1f5f9 !important;
        padding: 1rem 0.5rem !important;
        border-radius: 10px !important;
        text-align: center;
        transition: transform 0.1s ease;
    }
    
    [data-testid="stMetricContainer"]:hover {
        background-color: #f1f5f9 !important;
    }

    [data-testid="stMetricLabel"] {
        color: #64748b !important;
        font-size: 0.75rem !important;
        font-weight: 600 !important;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 4px !important;
    }

    [data-testid="stMetricValue"] {
        color: #1e293b !important;
        font-size: 1.6rem !important;
        font-weight: 700 !important;
    }

    /* Divider and Spacing */
    hr { margin: 1.5rem 0 !important; border-color: #f1f5f9 !important; }
    
    .stCaption { color: #94a3b8 !important; font-weight: 500 !important; }

    /* Hide password visibility toggle */
    button[aria-label="Show password text"],
    button[aria-label="Hide password text"],
    .stTextInput button[kind="icon"] {
        display: none !important;
    }
    
    /* Auto-refresh optimizations */
    .stApp > div:first-child { transition: none !important; }
    .element-container, .stMetric, .stPlotlyChart, .stContainer {
        transition: none !important;
        opacity: 1 !important;
    }
    [data-testid="stAppViewContainer"] * { transition: none !important; }
</style>
""", unsafe_allow_html=True)


CREDENTIALS_FILE = "credentials.enc"  # Encrypted file
KEY_FILE = ".secret.key"  # Hidden key file

def _get_or_create_key():
    """Get or create encryption key based on machine identifier."""
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f:
            return f.read()
    else:
        # Generate a new key
        key = Fernet.generate_key()
        with open(KEY_FILE, "wb") as f:
            f.write(key)
        # Make file hidden on Unix systems
        try:
            os.chmod(KEY_FILE, 0o600)  # Owner read/write only
        except:
            pass
        return key

def _get_cipher():
    """Get Fernet cipher instance."""
    key = _get_or_create_key()
    return Fernet(key)

def load_credentials():
    """Load and decrypt credentials from encrypted file."""
    if os.path.exists(CREDENTIALS_FILE):
        try:
            cipher = _get_cipher()
            with open(CREDENTIALS_FILE, "rb") as f:
                encrypted_data = f.read()
            decrypted_data = cipher.decrypt(encrypted_data)
            return json.loads(decrypted_data.decode('utf-8'))
        except Exception as e:
            # If decryption fails, return empty (corrupted or wrong key)
            return {}
    return {}

def save_credentials(client_id, client_secret, region):
    """Encrypt and save credentials to file."""
    cipher = _get_cipher()
    data = json.dumps({
        "client_id": client_id,
        "client_secret": client_secret,
        "region": region
    }).encode('utf-8')
    encrypted_data = cipher.encrypt(data)
    with open(CREDENTIALS_FILE, "wb") as f:
        f.write(encrypted_data)
    try:
        os.chmod(CREDENTIALS_FILE, 0o600)  # Owner read/write only
    except:
        pass

def delete_credentials():
    """Delete encrypted credentials file."""
    if os.path.exists(CREDENTIALS_FILE):
        os.remove(CREDENTIALS_FILE)
    # Also delete old unencrypted file if exists
    if os.path.exists("credentials.json"):
        os.remove("credentials.json")

# Initialize session state
if 'api_client' not in st.session_state:
    st.session_state.api_client = None
if 'users_map' not in st.session_state:
    st.session_state.users_map = {}
if 'queues_map' not in st.session_state:
    st.session_state.queues_map = {}
if 'users_info' not in st.session_state:
    st.session_state.users_info = {}
if 'language' not in st.session_state:
    st.session_state.language = "TR"
# --- HELPER: CONFIG PERSISTENCE ---
CONFIG_FILE = "dashboard_config.json"
PRESETS_FILE = "presets.json"

def load_dashboard_config():
    if os.path.exists(CONFIG_FILE):
        try:
            with open(CONFIG_FILE, "r") as f:
                return json.load(f)
        except:
            pass
    return {"layout": 1, "cards": []}

def save_dashboard_config(layout, cards):
    try:
        data = {"layout": layout, "cards": cards}
        with open(CONFIG_FILE, "w") as f:
            json.dump(data, f)
    except Exception as e:
        print(f"Error saving config: {e}")

def load_presets():
    """Load report presets from file."""
    if os.path.exists(PRESETS_FILE):
        try:
            with open(PRESETS_FILE, "r") as f:
                data = json.load(f)
                # Ensure it's the new list-based format
                if isinstance(data, list):
                    return data
                # Convert old dictionary format to new list format if needed
                return []
        except:
            pass
    return []

def save_presets(presets):
    """Save report presets to file."""
    try:
        with open(PRESETS_FILE, "w") as f:
            json.dump(presets, f)
    except Exception as e:
        print(f"Error saving presets: {e}")

def get_all_configs_json():
    """Combines all configurations into a single JSON for export."""
    dashboard = load_dashboard_config()
    presets = load_presets()
    return json.dumps({
        "dashboard": dashboard,
        "report_presets": presets
    }, indent=2)

def import_all_configs(json_data):
    """Restores configurations from a JSON string."""
    try:
        data = json.loads(json_data)
        if "dashboard" in data:
            save_dashboard_config(data["dashboard"].get("layout", 1), data["dashboard"].get("cards", []))
        if "report_presets" in data:
            save_presets(data["report_presets"])
        return True
    except Exception as e:
        print(f"Error importing configs: {e}")
        return False

# --- SIDEBAR ---
saved_creds = load_credentials()

with st.sidebar:
    st.session_state.language = st.selectbox("Dil / Language", ["TR", "EN"])
    lang = st.session_state.language
    st.title("Settings / Ayarlar")
    
    # Navigation
    st.session_state.page = st.radio(get_text(lang, "sidebar_title"), 
                                     [get_text(lang, "menu_reports"), get_text(lang, "menu_dashboard")])
    
    st.write("---")
    client_id = st.text_input("Client ID", value=saved_creds.get("client_id", ""), type="password")
    client_secret = st.text_input("Client Secret", value=saved_creds.get("client_secret", ""), type="password")
    
    regions = ["mypurecloud.ie", "mypurecloud.com", "mypurecloud.de"]
    saved_region = saved_creds.get("region", "mypurecloud.ie")
    region_index = regions.index(saved_region) if saved_region in regions else 0
    region = st.selectbox("Region", regions, index=region_index)
    
    remember_me = st.checkbox(get_text(lang, "remember_me"), value=bool(saved_creds))
    
    if st.button("Login"):
        if client_id and client_secret:
            with st.spinner("Authenticating..."):
                api_client, error = authenticate(client_id, client_secret, region)
                if api_client:
                    st.session_state.api_client = api_client
                    st.success("Login Success!")
                    
                    if remember_me:
                        save_credentials(client_id, client_secret, region)
                    else:
                        delete_credentials()
                    
                    gen_api = GenesysAPI(api_client)
                    users = gen_api.get_users()
                    st.session_state.users_map = {u['name']: u['id'] for u in users}
                    st.session_state.users_info = {u['id']: {'name': u['name'], 'username': u['username']} for u in users}
                    
                    queues = gen_api.get_queues()
                    st.session_state.queues_map = {q['name']: q['id'] for q in queues}
                    
                    # Fetch Presence Definitions for mapping UUIDs
                    st.session_state.presence_map = gen_api.get_presence_definitions()

                    st.rerun()
                    st.rerun()
                else:
                    st.error(f"Error: {error}")
    
    if st.session_state.api_client:
        st.write("---")
        if st.button("Logout"):
            st.session_state.api_client = None
            st.rerun()

    st.write("---")
    st.subheader(get_text(lang, "export_config"))
    st.download_button(
        label=get_text(lang, "export_config"),
        data=get_all_configs_json(),
        file_name=f"genesys_config_{datetime.now().strftime('%Y%m%d')}.json",
        mime="application/json",
        width='stretch'
    )
    
    st.write("---")
    st.subheader(get_text(lang, "import_config"))
    uploaded_file = st.file_uploader(get_text(lang, "import_config"), type=["json"])
    if uploaded_file is not None:
        if st.button(get_text(lang, "save"), key="import_btn"):
            content = uploaded_file.getvalue().decode("utf-8")
            if import_all_configs(content):
                st.success(get_text(lang, "config_imported"))
                # Clear dashboard cache so it reloads from file
                if 'dashboard_config_loaded' in st.session_state:
                    del st.session_state.dashboard_config_loaded
            else:
                st.error(get_text(lang, "config_import_error"))

# --- HELPER: PLOTLY CHARTS ---
def create_gauge_chart(value, title, height=250):
    fig = go.Figure(go.Indicator(
        mode = "gauge+number",
        value = value,
        title = {'text': title},
        gauge = {
            'axis': {'range': [0, 100]},
            'bar': {'color': "#00AEC7"}, # Genesys Style Blue
            'steps': [
                {'range': [0, 50], 'color': "#ffebee"},
                {'range': [50, 80], 'color': "#fff3e0"},
                {'range': [80, 100], 'color': "#e8f5e9"}],
        }
    ))
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=50, b=20))
    return fig

def create_donut_chart(data_dict, title, height=300):
    # Filter out zeros for cleaner chart
    filtered_data = {k: v for k, v in data_dict.items() if v > 0}
    if not filtered_data:
        filtered_data = {"N/A": 1} # Placeholder
        
    df = pd.DataFrame(list(filtered_data.items()), columns=['Status', 'Count'])
    
    fig = px.pie(df, values='Count', names='Status', title=title, hole=0.6,
                 color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=50, b=20))
    return fig


# --- MAIN LOGIC ---
lang = st.session_state.language

if not st.session_state.api_client:
    c_logo, c_title = st.columns([1, 4])
    with c_logo:
        pass # Placeholder for logo if needed
    with c_title:
        st.title(get_text(lang, "title"))
    st.info(get_text(lang, "welcome"))
else:
    # --- PAGE: REPORTS ---
    if st.session_state.page == get_text(lang, "menu_reports"):
        st.title(get_text(lang, "menu_reports"))

        # --- PRESET MANAGEMENT (NEW) ---
        presets = load_presets()
        preset_names = [p['name'] for p in presets]
        
        c_pre1, c_pre2, c_pre3 = st.columns([2, 1, 1])
        
        with c_pre1:
            selected_preset_name = st.selectbox(
                get_text(lang, "select_view"),
                [get_text(lang, "no_view_selected")] + preset_names,
                key="preset_selector"
            )
            
        if selected_preset_name != get_text(lang, "no_view_selected"):
            # Apply preset if selected
            preset = next((p for p in presets if p['name'] == selected_preset_name), None)
            if preset:
                # Explicitly update session state keys to force widgets to sync
                st.session_state.active_preset = preset
                st.session_state.rep_type = preset.get("type", "report_agent")
                st.session_state.rep_names = preset.get("names", [])
                st.session_state.rep_metrics = preset.get("metrics", DEFAULT_METRICS)
                st.session_state.rep_gran = preset.get("granularity_label", preset.get("granularity", "Toplam"))
                st.session_state.rep_fill = preset.get("fill_gaps", False)

        with st.expander(get_text(lang, "save_view")):
            c_sv1, c_sv2 = st.columns([3, 1], vertical_alignment="bottom")
            new_preset_name = c_sv1.text_input(get_text(lang, "preset_name"), placeholder="Günlük Raporum...")
            if c_sv2.button(get_text(lang, "save"), use_container_width=True, type="primary"):
                if new_preset_name:
                    # Collect current state from keys we'll define below
                    new_preset = {
                        "name": new_preset_name,
                        "type": st.session_state.get("rep_type", "agent_report"),
                        "names": st.session_state.get("rep_names", []),
                        "metrics": st.session_state.get("rep_metrics", DEFAULT_METRICS),
                        "granularity": gran_opt.get(st.session_state.get("rep_gran", get_text(lang, "total")), "P1D"),
                        "granularity_label": st.session_state.get("rep_gran", get_text(lang, "total")),
                        "fill_gaps": st.session_state.get("rep_fill", False)
                    }
                    # Update or Add
                    presets = [p for p in presets if p['name'] != new_preset_name]
                    presets.append(new_preset)
                    save_presets(presets)
                    st.success(get_text(lang, "view_saved"))
                    st.rerun()

        if selected_preset_name != get_text(lang, "no_view_selected"):
            c_del1, c_del2 = st.columns([1, 3])
            if c_del1.button(get_text(lang, "delete_view"), type="secondary", use_container_width=True):
                presets = [p for p in presets if p['name'] != selected_preset_name]
                save_presets(presets)
                st.info(get_text(lang, "view_deleted"))
                st.rerun()
        
        st.write("---")
        
        # Determine base values for widgets (preset or session)
        def_p = st.session_state.get("active_preset", {})
        
        c1, c2 = st.columns([1, 1])
        with c1:
            report_type_key = st.radio(
                get_text(lang, "report_type"),
                ["report_agent", "report_queue", "report_detailed"],
                format_func=lambda x: get_text(lang, x),
                horizontal=True,
                key="rep_type",
                index=["report_agent", "report_queue", "report_detailed"].index(def_p.get("type", "report_agent"))
            )
            report_type = "Agent" if report_type_key == "report_agent" else ("Workgroup" if report_type_key == "report_queue" else "Detailed")

        with c2:
            if report_type in ["Agent", "Detailed"]:
                selected_names = st.multiselect(
                    get_text(lang, "select_agents"), 
                    list(st.session_state.users_map.keys()),
                    key="rep_names",
                    default=def_p.get("names", []) if def_p.get("type") in ["report_agent", "report_detailed"] else []
                )
                selected_ids = [st.session_state.users_map[name] for name in selected_names]
            elif report_type == "Workgroup":
                selected_names = st.multiselect(
                    get_text(lang, "select_workgroups"), 
                    list(st.session_state.queues_map.keys()),
                    key="rep_names",
                    default=def_p.get("names", []) if def_p.get("type") == "report_queue" else []
                )
                selected_ids = [st.session_state.queues_map[name] for name in selected_names]

        # 2. Row: Dates and Times (UTC+3)
        st.write("---")
        c3, c4 = st.columns(2, vertical_alignment="bottom")
        with c3:
            d1, t1 = st.columns(2, vertical_alignment="bottom")
            start_date = d1.date_input("Start Date / Başlangıç", datetime.today())
            start_time = t1.time_input(get_text(lang, "start_time"), time(0, 0))
        with c4:
            d2, t2 = st.columns(2, vertical_alignment="bottom")
            end_date = d2.date_input("End Date / Bitiş", datetime.today())
            end_time = t2.time_input(get_text(lang, "end_time"), time(23, 59))
            
        # Granularity and Gap Filling
        g1, g2 = st.columns([1, 1], vertical_alignment="bottom")
        gran_opt = {
            get_text(lang, "total"): "P1D",
            get_text(lang, "30min"): "PT30M",
            get_text(lang, "1hour"): "PT1H"
        }
        
        # Match label string from value
        saved_gran = def_p.get("granularity", "P1D")
        if saved_gran in gran_opt.values():
            def_gran_label = list(gran_opt.keys())[list(gran_opt.values()).index(saved_gran)]
        else:
            def_gran_label = list(gran_opt.keys())[0]
        selected_gran_label = g1.selectbox(
            get_text(lang, "granularity"), 
            list(gran_opt.keys()),
            key="rep_gran",
            index=list(gran_opt.keys()).index(def_gran_label)
        )
        granularity = gran_opt[selected_gran_label]
        
        do_fill_gaps = g2.checkbox(
            get_text(lang, "fill_gaps"), 
            value=def_p.get("fill_gaps", False),
            key="rep_fill"
        )

          # Metrics Selection
        st.write("---")
        from src.lang import ALL_METRICS as ALL_M
        
        # Determine default metrics based on report type
        auto_def_metrics = ["nOffered", "nAnswered", "tAnswered", "tTalk", "tHandle"]
            
        def_metrics = def_p.get("metrics", auto_def_metrics)
        # Ensure all selected metrics exist in the list
        selection_options = ALL_M
        def_metrics = [m for m in def_metrics if m in selection_options]
        
        selected_metrics = st.multiselect(
            get_text(lang, "metrics"),
            selection_options,
            default=def_metrics,
            format_func=lambda x: get_text(lang, x),
            key="rep_metrics"
        )

        # 4. Action
        if st.button(get_text(lang, "fetch_report"), type="primary", width='stretch'):
            if not selected_metrics:
                st.warning("Lütfen metrik seçiniz.")
            else:
                with st.spinner(get_text(lang, "fetching_data")):
                    gen_api = GenesysAPI(st.session_state.api_client)
                    start_dt_local = datetime.combine(start_date, start_time)
                    end_dt_local = datetime.combine(end_date, end_time)
                    start_dt_utc = start_dt_local - timedelta(hours=3)
                    end_dt_utc = end_dt_local - timedelta(hours=3)
                    
                    group_by = ['userId'] if report_type == "Agent" else (['queueId'] if report_type == "Workgroup" else ['userId', 'queueId'])
                    filter_type = 'user' if report_type in ["Agent", "Detailed"] else 'queue'
                    
                    # Check if we have call metrics selected
                    call_metrics_selected = [m for m in selected_metrics if not m.startswith("t") or m in ["tTalk", "tAcw", "tHandle", "tHeld", "tWait", "tAcd", "tAlert", "tAnswered", "tAbandon", "tOutbound"]]
                    
                    api_response = None
                    if call_metrics_selected:
                        api_response = gen_api.get_analytics_conversations_aggregate(
                            start_dt_utc, end_dt_utc, granularity=granularity,
                            group_by=group_by, filter_type=filter_type, 
                            filter_ids=selected_ids if selected_ids else None, metrics=selected_metrics
                        )

                    if api_response and "error" in api_response:
                        st.error(f"API Error: {api_response['error']}")
                    else:
                        # Create queue lookup map (ID -> Name)
                        queue_lookup = {v: k for k, v in st.session_state.queues_map.items()}

                        if report_type in ["Agent", "Detailed"]:
                            lookup_map = st.session_state.users_info
                        else:
                            lookup_map = queue_lookup
                            
                        df = process_analytics_response(api_response, lookup_map, report_type.lower(), queue_map=queue_lookup)
                    
                    # If df is empty but it's an agent-based report, we should still create a base df for users
                    # If df is empty but it's an agent-based report, we should still create a base df for users
                    if df.empty and report_type in ["Agent", "Detailed"]:
                        agent_data = []
                        target_uids = selected_ids if selected_ids else list(st.session_state.users_info.keys())
                        for uid in target_uids:
                            uinfo = st.session_state.users_info.get(uid, {})
                            raw_user = uinfo.get('username', "")
                            row = {
                                "Name": uinfo.get('name', uid),
                                "Username": raw_user.split('@')[0] if raw_user else "",
                                "Id": uid
                            }
                            if report_type == "Detailed":
                                row["WorkgroupName"] = "-"
                                row["AgentName"] = row["Name"]
                                row["Id"] = f"{uid}|-"
                            agent_data.append(row)
                        df = pd.DataFrame(agent_data)
                    
                    # --- FETCH PRESENCE DATA IF REQUESTED ---
                    presence_keys = ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "col_staffed_time"]
                    detail_keys = ["col_login", "col_logout"]
                    
                    user_ids_to_query = selected_ids if selected_ids else list(st.session_state.users_info.keys())
                    
                    # Merge status durations
                    if any(m in selected_metrics for m in presence_keys) and report_type in ["Agent", "Detailed"]:
                        presence_resp = gen_api.get_user_aggregates(start_dt_utc, end_dt_utc, user_ids_to_query)
                        from src.processor import process_user_aggregates
                        presence_map = process_user_aggregates(presence_resp, st.session_state.get('presence_map'))
                        
                        if presence_map:
                            for pk in ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "StaffedTime"]:
                                col_name = pk if pk != "StaffedTime" else "col_staffed_time"
                                if not df.empty:
                                    uid_col = "Id"
                                    df[col_name] = df[uid_col].apply(lambda x: presence_map.get(x.split('|')[0] if '|' in x else x, {}).get(pk, 0))

                    # Merge Login/Logout details
                    if any(m in selected_metrics for m in detail_keys) and report_type in ["Agent", "Detailed"]:
                        details_resp = gen_api.get_user_status_details(start_dt_utc, end_dt_utc, user_ids_to_query)
                        from src.processor import process_user_details
                        details_map = process_user_details(details_resp)
                        
                        if details_map:
                            if not df.empty:
                                if "col_login" in selected_metrics:
                                    df["col_login"] = df["Id"].apply(lambda x: details_map.get(x.split('|')[0] if '|' in x else x, {}).get("Login", "N/A"))
                                if "col_logout" in selected_metrics:
                                    df["col_logout"] = df["Id"].apply(lambda x: details_map.get(x.split('|')[0] if '|' in x else x, {}).get("Logout", "N/A"))

                    if not df.empty:
                        if do_fill_gaps and granularity != "P1D":
                            df = fill_interval_gaps(df, start_dt_local, end_dt_local, granularity)
                            
                        # Final column filtering and renaming
                        if report_type == "Detailed":
                            base_cols = ["AgentName", "Username", "WorkgroupName"]
                        elif report_type in ["Agent", "Productivity"]:
                            base_cols = ["Name", "Username"]
                        else:
                            base_cols = ["Name"]

                        if "Interval" in df.columns: base_cols = ["Interval"] + base_cols
                        
                        # Prepare list of metrics to include (both from call and presence)
                        all_disp_metrics = selected_metrics + (["AvgHandle"] if "AvgHandle" in df.columns else [])
                        cols_to_keep = [c for c in base_cols if c in df.columns] + [m for m in all_disp_metrics if m in df.columns]
                        
                        df_final = df[cols_to_keep].copy()
                        
                        # Fix: Ensure ALL selected metrics exist in df_final even if they were missing from API
                        for sm in selected_metrics:
                            if sm not in df_final.columns:
                                df_final[sm] = 0
                                
                        # Re-calculate cols_to_keep to include newly added zeros
                        cols_to_keep = [c for c in base_cols if c in df_final.columns] + [m for m in all_disp_metrics if m in df_final.columns]
                        df_final = df_final[cols_to_keep]
                        
                        # Filter out rows with no data if requested (implicitly yes based on user feedback)
                        # We check if all selected metrics are 0 or Null
                        # metric_cols_in_df = [m for m in selected_metrics if m in df_final.columns]
                        # if metric_cols_in_df:
                        #     # Replace NaN with 0 for checking
                        #     df_check = df_final[metric_cols_in_df].fillna(0)
                        #     # Keep row if ANY metric is non-zero (or non-empty for strings like Login/Logout if we considered them, but usually they come with numeric presence)
                        #     # Actually, Login/Logout are strings. presence columns are numbers.
                        #     # If report is Productivity, we surely want to hide if everything is 0/NA.
                        #      
                        #     # Simple logic: fail if all numerics are 0 AND all strings are N/A or empty
                        #     def has_data(row):
                        #         for col in metric_cols_in_df:
                        #             val = row[col]
                        #             if isinstance(val, (int, float)) and val != 0: return True
                        #             if isinstance(val, str) and val not in ["", "N/A", "0"]: return True
                        #         return False
                        #         
                        #     # df_final = df_final[df_final.apply(has_data, axis=1)]
                        #     pass
                        
                        rename_map = {
                            "Interval": get_text(lang, "col_interval"),
                            "AgentName": get_text(lang, "col_agent"),
                            "Username": get_text(lang, "col_username"),
                            "WorkgroupName": get_text(lang, "col_workgroup"),
                            "Name": get_text(lang, "col_agent") if report_type == 'Agent' else get_text(lang, "col_workgroup"),
                            "AvgHandle": get_text(lang, "col_avg_handle"),
                            "col_staffed_time": get_text(lang, "col_staffed_time"),
                            "col_login": get_text(lang, "col_login"),
                            "col_logout": get_text(lang, "col_logout")
                        }
                        for m in selected_metrics: 
                            if m not in rename_map:
                                rename_map[m] = get_text(lang, m)
                                
                        st.dataframe(df_final.rename(columns=rename_map), width='stretch')
                        st.download_button(get_text(lang, "download_excel"), data=to_excel(df_final.rename(columns=rename_map)), 
                                           file_name=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                    else:
                        st.warning(get_text(lang, "no_data"))

    # --- PAGE: DASHBOARD (LIVE) ---
    else:
        # Load Config Once
        if 'dashboard_config_loaded' not in st.session_state:
            config = load_dashboard_config()
            st.session_state.dashboard_layout = config.get("layout", 1)
            loaded_cards = config.get("cards", [])
            if not loaded_cards:
                 loaded_cards = [{"id": 0, "title": "", "queues": [], "size": "medium"}]
            st.session_state.dashboard_cards = loaded_cards
            st.session_state.dashboard_config_loaded = True

        st.title(get_text(lang, "menu_dashboard"))

        # Top Control Bar
        c_ctrl1, c_ctrl2, c_ctrl3 = st.columns([1, 2, 1])
        
        with c_ctrl1:
             if st.button(get_text(lang, "add_group"), width='stretch'):
                new_id = max([c['id'] for c in st.session_state.dashboard_cards], default=-1) + 1
                st.session_state.dashboard_cards.append({"id": new_id, "title": "", "queues": [], "size": "medium"})
                save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
        
        with c_ctrl2:
            sub_c1, sub_c2 = st.columns([2, 3])
            with sub_c1:
                selected_layout = st.radio("Layout", [1, 2, 3], 
                                           format_func=lambda x: f"Grid: {x}", 
                                           index=[1, 2, 3].index(st.session_state.dashboard_layout),
                                           horizontal=True, label_visibility="collapsed")
                
                if selected_layout != st.session_state.dashboard_layout:
                    st.session_state.dashboard_layout = selected_layout
                    save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                    st.rerun()

            with sub_c2:
                mode_opts = ["Live", "Yesterday", "Date"]
                mode_labels = {
                    "Live": get_text(lang, "mode_live"),
                    "Yesterday": get_text(lang, "mode_yesterday"),
                    "Date": get_text(lang, "mode_date")
                }
                
                if 'dashboard_mode' not in st.session_state:
                    st.session_state.dashboard_mode = "Live"
                    
                selected_mode = st.radio("Mode", mode_opts, 
                                         format_func=lambda x: mode_labels[x],
                                         index=mode_opts.index(st.session_state.dashboard_mode),
                                         horizontal=True, label_visibility="collapsed", key="mode_selector")
                
                st.session_state.dashboard_mode = selected_mode

        with c_ctrl3:
             if st.session_state.dashboard_mode == "Date":
                 sel_date = st.date_input("Date", datetime.today(), label_visibility="collapsed")
                 st.session_state.dashboard_date = sel_date
             elif st.session_state.dashboard_mode == "Live":
                 auto_refresh = st.toggle(get_text(lang, "auto_refresh"), value=True)
             else:
                 st.write("")

        st.write("---")

        # 1. PRE-FETCH DATA
        all_selected_queues = set()
        for card in st.session_state.dashboard_cards:
            for q_name in card.get('queues', []):
                if q_name in st.session_state.queues_map:
                    all_selected_queues.add(st.session_state.queues_map[q_name])

        # Auto Refresh Trigger (Moved up)
        is_live = (st.session_state.dashboard_mode == "Live")
        if is_live and auto_refresh:
            st_autorefresh(interval=10000, key="data_refresh")

        obs_data_map = {}
        daily_data_map = {}
        gen_api = GenesysAPI(st.session_state.api_client)
        
        if all_selected_queues:
            q_ids = list(all_selected_queues)
            id_map = {v: k for k, v in st.session_state.queues_map.items() if v in q_ids}
            
            try:
                # Determine Interval
                query_interval = None
                if is_live:
                     now_local = datetime.now()
                     start_local = datetime.combine(now_local.date(), time(0, 0))
                     start_utc = start_local - timedelta(hours=3)
                     end_utc = datetime.now(timezone.utc)
                     query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                
                elif st.session_state.dashboard_mode == "Yesterday":
                    now_local = datetime.now()
                    yest_local = now_local - timedelta(days=1)
                    start_local = datetime.combine(yest_local.date(), time(0, 0))
                    end_local = datetime.combine(yest_local.date(), time(23, 59, 59))
                    start_utc = start_local - timedelta(hours=3)
                    end_utc = end_local - timedelta(hours=3)
                    query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                    
                elif st.session_state.dashboard_mode == "Date":
                    sel_date = st.session_state.get('dashboard_date', datetime.today())
                    start_local = datetime.combine(sel_date, time(0,0))
                    end_local = datetime.combine(sel_date, time(23,59,59))
                    start_utc = start_local - timedelta(hours=3)
                    end_utc = end_local - timedelta(hours=3)
                    query_interval = f"{start_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_utc.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"

                if is_live:
                    obs_response = gen_api.get_queue_observations(list(id_map.keys()))
                    # Pass the presence map to resolve UUIDs to status names
                    p_map = st.session_state.get('presence_map', {})
                    obs_data_list = process_observations(obs_response, id_map, presence_map=p_map)
                    obs_data_map = {item['Queue']: item for item in obs_data_list}
                
                # Cache daily stats for 60 seconds to reduce API calls
                cache_key = f"daily_cache_{st.session_state.dashboard_mode}_{query_interval}"
                cache_time_key = f"{cache_key}_time"
                
                current_time = pytime.time()
                if cache_key not in st.session_state or (current_time - st.session_state.get(cache_time_key, 0)) > 60:
                    daily_response = gen_api.get_queue_daily_stats(list(id_map.keys()), interval=query_interval)
                    daily_data_map = process_daily_stats(daily_response, id_map)
                    st.session_state[cache_key] = daily_data_map
                    st.session_state[cache_time_key] = current_time
                else:
                    daily_data_map = st.session_state[cache_key]
                
            except Exception as e:
                pass

        # 2. RENDER LAYOUT & CARDS
        grid_cols = st.columns(st.session_state.dashboard_layout)
        cards_to_remove = []

        for idx, card in enumerate(st.session_state.dashboard_cards):
            col_index = idx % st.session_state.dashboard_layout
            with grid_cols[col_index]:
                with st.container(border=True):
                    # --- Header Bar ---
                    disp_title = card['title'] if card['title'] else f"Grup #{card['id']+1}"
                    st.markdown(f"### {disp_title}")
                    
                    # --- Settings Expander ---
                    with st.expander(f"⚙️ {get_text(lang, 'add_group').replace('➕ ', '')} / Ayarlar", expanded=False):
                        new_title = st.text_input(get_text(lang, "group_title_placeholder"), value=card['title'], key=f"title_{card['id']}")
                        if new_title != card.get('title'):
                            card['title'] = new_title
                            save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                            st.rerun()
                        
                        new_queues = st.multiselect(get_text(lang, "select_queues_for_group"), 
                                                    list(st.session_state.queues_map.keys()), 
                                                    default=card.get('queues', []),
                                                    key=f"q_{card['id']}")
                        if new_queues != card.get('queues'):
                            card['queues'] = new_queues
                            save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                            st.rerun()
                        
                        size_opts = {get_text(lang, "size_small"): "small", 
                                     get_text(lang, "size_medium"): "medium", 
                                     get_text(lang, "size_large"): "large"}
                        curr_size = card.get('size', 'medium')
                        curr_label = list(size_opts.keys())[list(size_opts.values()).index(curr_size)] if curr_size in size_opts.values() else get_text(lang, "size_medium")
                        
                        new_size_label = st.selectbox(get_text(lang, "card_size"), list(size_opts.keys()), 
                                                      index=list(size_opts.keys()).index(curr_label),
                                                      key=f"size_{card['id']}")
                        new_size = size_opts[new_size_label]
                        if new_size != card.get('size'):
                            card['size'] = new_size
                            save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                            st.rerun()
                            
                        st.write("")
                        if st.button(get_text(lang, "delete_group"), key=f"del_{card['id']}"):
                            cards_to_remove.append(idx)
                    
                    # --- CONTENT RENDERING ---
                    if not card.get('queues'):
                        st.info("Kuyruk seçiniz / Select queues")
                        continue

                    card_items_live = [obs_data_map.get(q_name) for q_name in card['queues'] if obs_data_map.get(q_name)] if is_live else []
                    card_items_daily = [daily_data_map.get(q_name) for q_name in card['queues'] if daily_data_map.get(q_name)]
                    
                    total_waiting = sum(d['Waiting'] for d in card_items_live) if card_items_live else 0
                    total_interacting = sum(d['Interacting'] for d in card_items_live) if card_items_live else 0
                    num_queues = len(card_items_live) if card_items_live else 1
                    
                    # Kuyrukta Agent = OnQueueIdle + OnQueueInteracting (toplam kuyrukta olan)
                    total_on_queue_idle = sum(d.get('OnQueueIdle', 0) for d in card_items_live) if card_items_live else 0
                    total_on_queue_interacting = sum(d.get('OnQueueInteracting', 0) for d in card_items_live) if card_items_live else 0
                    total_on_queue_raw = total_on_queue_idle + total_on_queue_interacting
                    on_queue_agents = round(total_on_queue_raw / num_queues) if num_queues > 0 else 0
                    
                    # Agent = OnQueueIdle (kuyrukta ve çağrıda olmayan - hazır agentlar)
                    available_agents = round(total_on_queue_idle / num_queues) if num_queues > 0 else 0

                    num_selected = len(card['queues']) if card['queues'] else 1
                    
                    # Görüşmede: Görüşmesi 0'dan fazla olan kuyrukları dikkate alarak ortalama al
                    interacting_list = [d['Interacting'] for d in card_items_live if d['Interacting'] > 0]
                    status_interacting_val = round(sum(interacting_list) / len(interacting_list)) if interacting_list else 0

                    # Müsait olan agent sayısı (Presence based: Available) - Seçili kuyruk sayısına böl
                    total_available_presence = sum(d['Presences'].get('Available', 0) for d in card_items_live) if card_items_live else 0
                    available_presence_val = round(total_available_presence / num_selected) if num_selected > 0 else 0
                    
                    # On Queue (Kuyruktaki Agent) - Seçili kuyruk sayısına böl
                    total_on_queue_presence = sum(d['Presences'].get('On Queue', 0) for d in card_items_live) if card_items_live else 0
                    on_queue_presence_val = round(total_on_queue_presence / num_selected) if num_selected > 0 else 0
                    
                    avg_sl_live = sum(d['ServiceLevel'] for d in card_items_live) / len(card_items_live) if card_items_live else 0
                    
                    total_offered = sum(d['Offered'] for d in card_items_daily) if card_items_daily else 0
                    total_answered = sum(d['Answered'] for d in card_items_daily) if card_items_daily else 0
                    total_abandoned = sum(d['Abandoned'] for d in card_items_daily) if card_items_daily else 0
                    total_sl_num = sum(d.get('SL_Numerator', 0) for d in card_items_daily) if card_items_daily else 0
                    total_sl_den = sum(d.get('SL_Denominator', 0) for d in card_items_daily) if card_items_daily else 0
                    
                    answer_rate = (total_answered / total_offered * 100) if total_offered > 0 else 0
                    daily_sl = (total_sl_num / total_sl_den * 100) if total_sl_den > 0 else 0
                    
                    c_size = card.get('size', 'medium')
                    h_gauge = 180 if c_size == 'small' else (250 if c_size == 'medium' else 300)
                    gauge_value = daily_sl if daily_sl > 0 or total_offered > 0 else (avg_sl_live if is_live else 0)
                    gauge_key = f"g_gage_{card['id']}_{idx}"

                    if is_live:
                        st.caption(f"🔵 {get_text(lang, 'live_stat')}")
                        m_cols = st.columns(5) 
                        m_cols[0].metric(get_text(lang, "waiting"), total_waiting)
                        m_cols[1].metric(get_text(lang, "interacting"), status_interacting_val)
                        m_cols[2].metric(get_text(lang, "agent"), on_queue_presence_val)
                        m_cols[3].metric(get_text(lang, "on_queue_agents"), on_queue_presence_val)
                        m_cols[4].metric(get_text(lang, "available_agents"), available_presence_val)
                        st.write("")
                    else:
                        st.info(get_text(lang, "no_live_data"))

                    st.divider()

                    if c_size == 'small':
                        st.caption(f"📅 {get_text(lang, 'daily_stat')}")
                        d_cols = st.columns(4)
                        d_cols[0].metric(get_text(lang, "offered"), total_offered)
                        d_cols[1].metric(get_text(lang, "answered"), total_answered)
                        d_cols[2].metric(get_text(lang, "abandoned"), total_abandoned)
                        d_cols[3].metric(get_text(lang, "answer_rate"), f"%{answer_rate:.1f}")
                        st.plotly_chart(create_gauge_chart(gauge_value, get_text(lang, "avg_service_level"), height=h_gauge), width='stretch', key=gauge_key)
                    else:
                        k1, k2 = st.columns([1, 1])
                        with k1:
                            st.caption(f"📅 {get_text(lang, 'daily_stat')}")
                            dm1, dm2 = st.columns(2)
                            dm1.metric(get_text(lang, "offered"), total_offered)
                            dm1.metric(get_text(lang, "answered"), total_answered)
                            dm2.metric(get_text(lang, "abandoned"), total_abandoned)
                            dm2.metric(get_text(lang, "answer_rate"), f"%{answer_rate:.1f}")
                        with k2:
                            st.plotly_chart(create_gauge_chart(gauge_value, get_text(lang, "avg_service_level"), height=h_gauge), width='stretch', key=gauge_key)

        # Handle Card Deletion after render
        if cards_to_remove:
            for index in sorted(cards_to_remove, reverse=True):
                del st.session_state.dashboard_cards[index]
            save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
            st.rerun()
