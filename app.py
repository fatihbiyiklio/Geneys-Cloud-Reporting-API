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
from src.data_manager import DataManager
from src.auth_manager import AuthManager

# --- AUTH MANAGER ---
@st.cache_resource
def get_auth_manager():
    return AuthManager()

auth_manager = get_auth_manager()

# --- BACKGROUND MONITOR ---
def _monitor_sessions():
    """Shuts down the process when no active sessions are left."""
    pytime.sleep(10)
    while True:
        try:
            runtime = Runtime.instance()
            session_count = len(runtime._session_mgr.list_active_sessions())
            if session_count == 0:
                pytime.sleep(10)
                session_count = len(runtime._session_mgr.list_active_sessions())
                if session_count == 0:
                    os._exit(0)
        except Exception:
            pass
        pytime.sleep(5)

if not any(t.name == "SessionMonitor" for t in threading.enumerate()):
    threading.Thread(target=_monitor_sessions, name="SessionMonitor", daemon=True).start()

# --- IMPORTS & PATHS ---
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
from src.lang import get_text, STRINGS, DEFAULT_METRICS, ALL_METRICS
from src.auth import authenticate
from src.api import GenesysAPI
from src.processor import process_analytics_response, to_excel, fill_interval_gaps, process_observations, process_daily_stats, process_user_aggregates, process_user_details, process_conversation_details, apply_duration_formatting

# --- CONFIGURATION ---
st.set_page_config(page_title="Genesys Cloud Reporting", layout="wide")

CREDENTIALS_FILE = "credentials.enc"
KEY_FILE = ".secret.key"
CONFIG_FILE = "dashboard_config.json"
PRESETS_FILE = "presets.json"

st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');
    html, body, [data-testid="stAppViewContainer"] { font-family: 'Inter', sans-serif !important; background-color: #ffffff !important; }
    [data-testid="stVerticalBlockBorderWrapper"] { background-color: #ffffff !important; border: 1px solid #eef2f6 !important; border-radius: 12px !important; padding: 1.5rem !important; margin-bottom: 1.5rem !important; }
    [data-testid="stMetricContainer"] { background-color: #f8fafb !important; border: 1px solid #f1f5f9 !important; padding: 1rem 0.5rem !important; border-radius: 10px !important; text-align: center; }
    [data-testid="stMetricContainer"]:hover { background-color: #f1f5f9 !important; }
    [data-testid="stMetricLabel"] { color: #64748b !important; font-size: 0.75rem !important; font-weight: 600 !important; text-transform: uppercase; letter-spacing: 0.05em; }
    [data-testid="stMetricValue"] { color: #1e293b !important; font-size: 1.6rem !important; font-weight: 700 !important; }
    hr { margin: 1.5rem 0 !important; border-color: #f1f5f9 !important; }
    button[aria-label="Show password text"], button[aria-label="Hide password text"] { display: none !important; }
</style>
""", unsafe_allow_html=True)

# --- GLOBAL HELPERS (DEFINED FIRST) ---

APP_SESSION_FILE = ".session.enc"

def _get_or_create_key():
    if os.path.exists(KEY_FILE):
        with open(KEY_FILE, "rb") as f: return f.read()
    key = Fernet.generate_key()
    with open(KEY_FILE, "wb") as f: f.write(key)
    try: os.chmod(KEY_FILE, 0o600)
    except: pass
    return key

def _get_cipher():
    return Fernet(_get_or_create_key())

def load_credentials():
    if not os.path.exists(CREDENTIALS_FILE): return {}
    try:
        cipher = _get_cipher()
        with open(CREDENTIALS_FILE, "rb") as f: data = f.read()
        return json.loads(cipher.decrypt(data).decode('utf-8'))
    except: return {}

def save_credentials(client_id, client_secret, region):
    cipher = _get_cipher()
    data = json.dumps({"client_id": client_id, "client_secret": client_secret, "region": region}).encode('utf-8')
    with open(CREDENTIALS_FILE, "wb") as f: f.write(cipher.encrypt(data))
    try: os.chmod(CREDENTIALS_FILE, 0o600)
    except: pass

def delete_credentials():
    if os.path.exists(CREDENTIALS_FILE): os.remove(CREDENTIALS_FILE)

def generate_password(length=12):
    """Generate a secure random password."""
    import secrets
    import string
    alphabet = string.ascii_letters + string.digits + "!@#$%"
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# --- APP SESSION MANAGEMENT (REMEMBER ME) ---
def load_app_session():
    if not os.path.exists(APP_SESSION_FILE): return None
    try:
        cipher = _get_cipher()
        with open(APP_SESSION_FILE, "rb") as f: data = f.read()
        session_data = json.loads(cipher.decrypt(data).decode('utf-8'))
        
        # Check 3-hour expiry
        timestamp = session_data.get("timestamp", 0)
        if pytime.time() - timestamp > (3 * 3600):
            delete_app_session()
            return None
            
        return session_data
    except Exception:
        return None

def save_app_session(user_data):
    try:
        cipher = _get_cipher()
        # Add timestamp
        payload = {**user_data, "timestamp": pytime.time()}
        data = json.dumps(payload).encode('utf-8')
        with open(APP_SESSION_FILE, "wb") as f: f.write(cipher.encrypt(data))
        try: os.chmod(APP_SESSION_FILE, 0o600)
        except: pass
    except: pass

def delete_app_session():
    if os.path.exists(APP_SESSION_FILE): os.remove(APP_SESSION_FILE)

def load_dashboard_config():
    if not os.path.exists(CONFIG_FILE): return {"layout": 1, "cards": []}
    try:
        with open(CONFIG_FILE, "r") as f: return json.load(f)
    except: return {"layout": 1, "cards": []}

def save_dashboard_config(layout, cards):
    try:
        with open(CONFIG_FILE, "w") as f: json.dump({"layout": layout, "cards": cards}, f)
    except: pass

def load_presets():
    if not os.path.exists(PRESETS_FILE): return []
    try:
        with open(PRESETS_FILE, "r") as f:
            data = json.load(f)
            return data if isinstance(data, list) else []
    except: return []

def save_presets(presets):
    try:
        with open(PRESETS_FILE, "w") as f: json.dump(presets, f)
    except: pass

def get_all_configs_json():
    return json.dumps({"dashboard": load_dashboard_config(), "report_presets": load_presets()}, indent=2)

def import_all_configs(json_data):
    try:
        data = json.loads(json_data)
        if "dashboard" in data:
            save_dashboard_config(data["dashboard"].get("layout", 1), data["dashboard"].get("cards", []))
        if "report_presets" in data:
            save_presets(data["report_presets"])
        return True
    except: return False

@st.cache_resource
def get_data_manager():
    return DataManager()

data_manager = get_data_manager()

def create_gauge_chart(value, title, height=250):
    fig = go.Figure(go.Indicator(
        mode = "gauge+number", value = value, title = {'text': title},
        gauge = {'axis': {'range': [0, 100]}, 'bar': {'color': "#00AEC7"},
                 'steps': [{'range': [0, 50], 'color': "#ffebee"}, {'range': [50, 80], 'color': "#fff3e0"}, {'range': [80, 100], 'color': "#e8f5e9"}]}))
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=50, b=20))
    return fig

def create_donut_chart(data_dict, title, height=300):
    filtered_data = {k: v for k, v in data_dict.items() if v > 0} or {"N/A": 1}
    fig = px.pie(pd.DataFrame(list(filtered_data.items()), columns=['Status', 'Count']), 
                 values='Count', names='Status', title=title, hole=0.6, color_discrete_sequence=px.colors.qualitative.Set2)
    fig.update_layout(height=height, margin=dict(l=20, r=20, t=50, b=20))
    return fig
# --- INITIALIZATION ---
if 'api_client' not in st.session_state: st.session_state.api_client = None
if 'users_map' not in st.session_state: st.session_state.users_map = {}
if 'queues_map' not in st.session_state: st.session_state.queues_map = {}
if 'users_info' not in st.session_state: st.session_state.users_info = {}
if 'language' not in st.session_state: st.session_state.language = "TR"
if 'app_user' not in st.session_state: st.session_state.app_user = None
if 'wrapup_map' not in st.session_state: st.session_state.wrapup_map = {}

# --- APP LOGIN ---
if not st.session_state.app_user:
    # Try Auto-Login from Encrypted Session File
    saved_session = load_app_session()
    if saved_session:
        st.session_state.app_user = saved_session
        st.rerun()

    st.markdown("<h1 style='text-align: center;'>Genesys Reporting API</h1>", unsafe_allow_html=True)
    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("app_login_form"):
            st.subheader("Uygulama Girişi / App Login")
            u_name = st.text_input("Kullanıcı Adı / Username")
            u_pass = st.text_input("Şifre / Password", type="password")
            remember_me = st.checkbox("Beni Hatırla / Remember Me")
            
            if st.form_submit_button("Giriş / Login", use_container_width=True):
                user_data = auth_manager.authenticate(u_name, u_pass)
                if user_data:
                    # Exclude password hash from session state and file
                    safe_user_data = {k: v for k, v in user_data.items() if k != 'password'}
                    full_user = {"username": u_name, **safe_user_data}
                    
                    # Handle Remember Me
                    if remember_me:
                        save_app_session(full_user)
                    else:
                        delete_app_session()
                        
                    st.session_state.app_user = full_user
                    st.rerun()
                else:
                    st.error("Hatalı kullanıcı adı veya şifre!")
    st.stop()

# --- AUTO-LOGIN ---

saved_creds = load_credentials()
if not st.session_state.api_client and saved_creds:
    cid, csec, reg = saved_creds.get("client_id"), saved_creds.get("client_secret"), saved_creds.get("region", "mypurecloud.ie")
    if cid and csec:
        client, err = authenticate(cid, csec, reg)
        if client:
            st.session_state.api_client = client
            api = GenesysAPI(client)
            users = api.get_users()
            st.session_state.users_map = {u['name']: u['id'] for u in users}
            st.session_state.users_info = {u['id']: {'name': u['name'], 'username': u['username']} for u in users}
            st.session_state.queues_map = {q['name']: q['id'] for q in api.get_queues()}
            st.session_state.wrapup_map = api.get_wrapup_codes()
            st.session_state.presence_map = api.get_presence_definitions()
            data_manager.update_api_client(client, st.session_state.presence_map)
            data_manager.start(st.session_state.queues_map)

# --- SIDEBAR ---
with st.sidebar:
    st.session_state.language = st.selectbox("Dil / Language", ["TR", "EN"])
    lang = st.session_state.language
    st.write(f"Hoş geldiniz, **{st.session_state.app_user['username']}** ({st.session_state.app_user['role']})")
    if st.button("Çıkış Yap / Logout App", type="secondary", use_container_width=True):
        st.session_state.app_user = None
        delete_app_session()
        st.rerun()
    st.title("Settings / Ayarlar")
    
    # Define menu options based on role
    menu_options = []
    role = st.session_state.app_user['role']
    if role in ["Admin", "Manager", "Reports User"]:
        menu_options.append(get_text(lang, "menu_reports"))
    if role in ["Admin", "Manager", "Dashboard User"]:
        menu_options.append(get_text(lang, "menu_dashboard"))
    if role == "Admin":
        menu_options.append("Kullanıcı Yönetimi")

    st.session_state.page = st.radio(get_text(lang, "sidebar_title"), menu_options)
    st.write("---")
    
    # Admin-only Credentials Section
    if role == "Admin":
        st.subheader("Genesys API Credentials")
        c_id = st.text_input("Client ID", value=saved_creds.get("client_id", ""), type="password")
        c_sec = st.text_input("Client Secret", value=saved_creds.get("client_secret", ""), type="password")
        regions = ["mypurecloud.ie", "mypurecloud.com", "mypurecloud.de"]
        region = st.selectbox("Region", regions, index=regions.index(saved_creds.get("region", "mypurecloud.ie")) if saved_creds.get("region") in regions else 0)
        remember = st.checkbox(get_text(lang, "remember_me"), value=bool(saved_creds))
        
        if st.button("Login (Genesys)"):
            if c_id and c_sec:
                with st.spinner("Authenticating..."):
                    client, err = authenticate(c_id, c_sec, region)
                    if client:
                        st.session_state.api_client = client
                        if remember: save_credentials(c_id, c_sec, region)
                        else: delete_credentials()
                        api = GenesysAPI(client)
                        users = api.get_users()
                        st.session_state.users_map = {u['name']: u['id'] for u in users}
                        st.session_state.users_info = {u['id']: {'name': u['name'], 'username': u['username']} for u in users}
                        st.session_state.queues_map = {q['name']: q['id'] for q in api.get_queues()}
                        st.session_state.wrapup_map = api.get_wrapup_codes()
                        st.session_state.presence_map = api.get_presence_definitions()
                        data_manager.update_api_client(client, st.session_state.presence_map)
                        data_manager.start(st.session_state.queues_map)
                        st.rerun()
                    else: st.error(f"Error: {err}")
    
    # Keep the Genesys Logout if needed, or hide it if not Admin?
    # Actually, once logged in to Genesys, it's global for the app session.
    # But only Admin can CHANGE it.
    
    if st.session_state.api_client and role == "Admin":
        if st.button("Logout (Genesys)"):
            st.session_state.api_client = None
            st.rerun()

    st.write("---")
    st.subheader(get_text(lang, "export_config"))
    st.download_button(label=get_text(lang, "export_config"), data=get_all_configs_json(), file_name=f"genesys_config_{datetime.now().strftime('%Y%m%d')}.json", mime="application/json")
    
    st.write("---")
    st.subheader(get_text(lang, "import_config"))
    up_file = st.file_uploader(get_text(lang, "import_config"), type=["json"])
    if up_file and st.button(get_text(lang, "save"), key="import_btn"):
        if import_all_configs(up_file.getvalue().decode("utf-8")):
            st.success(get_text(lang, "config_imported"))
            if 'dashboard_config_loaded' in st.session_state: del st.session_state.dashboard_config_loaded
        else: st.error(get_text(lang, "config_import_error"))

# --- MAIN LOGIC ---
if not st.session_state.api_client:
    st.title(get_text(lang, "title"))
    st.info(get_text(lang, "welcome"))
else:
    if st.session_state.page == get_text(lang, "menu_reports"):
        st.title(get_text(lang, "menu_reports"))
        presets = load_presets()
        c_p1, c_p2, c_p3 = st.columns([2, 1, 1])
        with c_p1:
            sel_p = st.selectbox(get_text(lang, "select_view"), [get_text(lang, "no_view_selected")] + [p['name'] for p in presets], key="preset_selector")
        
        def_p = {}
        if sel_p != get_text(lang, "no_view_selected"):
            p = next((p for p in presets if p['name'] == sel_p), None)
            if p:
                def_p = p
                for k in ["type", "names", "metrics", "granularity_label", "fill_gaps"]:
                    if k in p: st.session_state[f"rep_{k[:3]}"] = p[k]
        
        with st.expander(get_text(lang, "save_view")):
            cs1, cs2 = st.columns([3, 1], vertical_alignment="bottom")
            p_name = cs1.text_input(get_text(lang, "preset_name"))
            if cs2.button(get_text(lang, "save"), use_container_width=True, type="primary") and p_name:
                new_p = {"name": p_name, "type": st.session_state.get("rep_typ", "report_agent"), "names": st.session_state.get("rep_nam", []), "metrics": st.session_state.get("rep_met", DEFAULT_METRICS), "granularity_label": st.session_state.get("rep_gra", "Toplam"), "fill_gaps": st.session_state.get("rep_fil", False)}
                presets = [p for p in presets if p['name'] != p_name] + [new_p]
                save_presets(presets); st.success(get_text(lang, "view_saved")); st.rerun()

        st.write("---")
        c1, c2 = st.columns(2)
        with c1:
            role = st.session_state.app_user['role']
            if role == "Reports User":
                rep_types = ["report_agent", "report_queue", "report_detailed", "interaction_search", "missed_interactions"]
            else: # Admin, Manager
                rep_types = ["report_agent", "report_queue", "report_detailed", "interaction_search", "chat_detail", "missed_interactions"]
            
            r_type = st.selectbox(
                get_text(lang, "report_type"), 
                rep_types, 
                format_func=lambda x: get_text(lang, x),
                key="rep_typ", index=rep_types.index(def_p.get("type", "report_agent")) if def_p.get("type", "report_agent") in rep_types else 0)
        with c2:
            is_agent = r_type in ["report_agent", "report_detailed"]
            opts = list(st.session_state.users_map.keys()) if is_agent else list(st.session_state.queues_map.keys())
            sel_names = st.multiselect(get_text(lang, "select_agents" if is_agent else "select_workgroups"), opts, key="rep_nam")
            sel_ids = [(st.session_state.users_map if is_agent else st.session_state.queues_map)[n] for n in sel_names]

        st.write("---")
        c3, c4 = st.columns(2)
        with c3:
            d1, t1 = st.columns(2); sd = d1.date_input("Start Date", datetime.today()); st_ = t1.time_input(get_text(lang, "start_time"), time(0, 0))
        with c4:
            d2, t2 = st.columns(2); ed = d2.date_input("End Date", datetime.today()); et = t2.time_input(get_text(lang, "end_time"), time(23, 59))
        
        g1, g2 = st.columns(2)
        gran_opt = {get_text(lang, "total"): "P1D", get_text(lang, "30min"): "PT30M", get_text(lang, "1hour"): "PT1H"}
        sel_gran = g1.selectbox(get_text(lang, "granularity"), list(gran_opt.keys()), key="rep_gra")
        do_fill = g2.checkbox(get_text(lang, "fill_gaps"), key="rep_fil")

        st.write("---")
        
        # Filter metrics based on user permissions
        user_metrics = st.session_state.app_user.get('metrics', [])
        selection_options = user_metrics if user_metrics and role != "Admin" else ALL_METRICS
        
        if r_type not in ["interaction_search", "chat_detail", "missed_interactions"]:
            # Use last used metrics as default if available, otherwise use standard defaults
            if "last_metrics" in st.session_state and st.session_state.last_metrics:
                auto_def_metrics = [m for m in st.session_state.last_metrics if m in selection_options]
            else:
                auto_def_metrics = [m for m in ["nOffered", "nAnswered", "tAnswered", "tTalk", "tHandle"] if m in selection_options]
            sel_mets = st.multiselect(get_text(lang, "metrics"), selection_options, default=auto_def_metrics, format_func=lambda x: get_text(lang, x), key="rep_met")
        else:
            sel_mets = []


        if r_type == "chat_detail":
            st.info("Bu rapor, webchat, whatsapp, message gibi yazılı etkileşimlerin detaylarını ve 'Participant Data' verilerini içerir.")
            if st.button("Chat Verilerini Getir / Fetch Chat Data", type="primary", use_container_width=True):
                 with st.spinner(get_text(lang, "fetching_data")):
                     start_date = datetime.combine(sd, st_) - timedelta(hours=3)
                     end_date = datetime.combine(ed, et) - timedelta(hours=3)
                     
                     api = GenesysAPI(st.session_state.api_client)
                     # Fetch all, but we will filter for chats ideally or just process all with attributes
                     # Interaction detail endpoint returns all types.
                     raw_convs = api.get_conversation_details(start_date, end_date)
                     # Process with attributes=True (Base structure)
                     df = process_conversation_details(
                         raw_convs, 
                         st.session_state.users_info, 
                         st.session_state.queues_map, 
                         st.session_state.wrapup_map,
                         include_attributes=True
                     )
                     
                     if not df.empty:
                         # Filter for Chat/Message types FIRST to reduce API calls
                         chat_types = ['chat', 'message', 'webchat', 'whatsapp', 'facebook', 'twitter', 'line', 'telegram']
                         df_chat = df[df['MediaType'].isin(chat_types)].copy()

                         if not df_chat.empty:
                             st.info(f"Detay veriler {len(df_chat)} kayıt için çekiliyor... (Bu işlem biraz sürebilir) / Fetching detailed attributes...")
                             
                             # Create a progress bar
                             progress_bar = st.progress(0)
                             total_chats = len(df_chat)
                             
                             # Prepare a list to collect updated attributes
                             enrichment_data = []

                             # Use the helper to fetch
                             api_instance = GenesysAPI(st.session_state.api_client)

                             for index, (idx, row) in enumerate(df_chat.iterrows()):
                                 conv_id = row['Id']
                                 # Update progress
                                 progress_bar.progress((index + 1) / total_chats)
                                 
                                 try:
                                     # ENRICHMENT: Call Standard Conversation API
                                     # This is necessary because Analytics API often omits 'attributes' (Participant Data)
                                     full_conv = api_instance._get(f"/api/v2/conversations/{conv_id}")
                                     
                                     attrs = {}
                                     if full_conv and 'participants' in full_conv:
                                         # Look for customer participant who usually holds the attributes
                                         # Prioritize customer, then check others
                                         found_attrs = False
                                         for p in full_conv['participants']:
                                             if p.get('purpose') == 'customer' and 'attributes' in p and p['attributes']:
                                                 attrs = p['attributes']
                                                 found_attrs = True
                                                 break
                                         
                                         if not found_attrs:
                                              for p in full_conv['participants']:
                                                  if 'attributes' in p and p['attributes']:
                                                      attrs = p['attributes']
                                                      break
                                     
                                     # Append dict to list
                                     enrichment_data.append(attrs)
                                     
                                 except Exception as e:
                                     # If individual fetch fails
                                     enrichment_data.append({})
                             
                             progress_bar.empty()
                             
                             # Merge attributes into the DataFrame
                             # usage of 'at' for direct assignment
                             for i, attrs in enumerate(enrichment_data):
                                 original_index = df_chat.index[i]
                                 for k, v in attrs.items():
                                     # Determine content to set. If it's a date or number, pandas might complain if column is object type,
                                     # but usually it handles it. 
                                     # We cast to string if needed to be safe? No, let pandas handle types.
                                     df_chat.at[original_index, k] = v
                         
                         if df_chat.empty and not df.empty:
                             st.warning("Seçilen tarih aralığında hiç 'Chat/Mesaj' kaydı bulunamadı. (Sesli çağrılar hariç tutuldu)")
                         elif not df_chat.empty:
                             # Display
                             st.dataframe(df_chat, use_container_width=True)
                             st.download_button(get_text(lang, "download_excel"), data=to_excel(df_chat), file_name=f"chat_detail_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                         else:
                             st.warning(get_text(lang, "no_data"))
                     else:
                         st.warning(get_text(lang, "no_data"))

        # --- MISSED INTERACTIONS REPORT ---
        if r_type == "missed_interactions":
            st.info("Bu rapor, seçilen tarih aralığındaki CEVAPLANMAYAN (Kaçan) sesli ve yazılı etkileşimleri listeler.")
            
            # Dynamic Column Selection (Reuse interaction cols)
            from src.lang import INTERACTION_COLUMNS
            default_cols = [c for c in INTERACTION_COLUMNS if c not in ["col_attributes", "col_media"]]
             # Let user select
            selected_cols_keys = st.multiselect("Sütunları Seçin / Select Columns", INTERACTION_COLUMNS, default=INTERACTION_COLUMNS, format_func=lambda x: get_text(lang, x))

            if st.button("Kaçan Çağrıları Getir / Fetch Missed", type="primary", use_container_width=True):
                 with st.spinner(get_text(lang, "fetching_data")):
                     # Fetch data
                     # We need to fetch conversation details
                     # api = GenesysAPI(st.session_state.api_client) # already initialized above if needed, but let's re-init
                     api = GenesysAPI(st.session_state.api_client)
                     
                     s_dt = datetime.combine(sd, st_) - timedelta(hours=3)
                     e_dt = datetime.combine(ed, et) - timedelta(hours=3)
                     
                     # Get details
                     raw_data = api.get_conversation_details(s_dt, e_dt)
                     
                     # DEBUG: Dump first 5 conversations to check attributes
                     try:
                         import json
                         with open("debug_chat_dump.json", "w", encoding="utf-8") as f:
                             json.dump(raw_data.get('conversations', [])[:5], f, indent=2, default=str)
                     except: pass
                     
                     df = process_conversation_details(
                         raw_data, 
                         user_map=st.session_state.users_info, 
                         queue_map=st.session_state.queues_map, 
                         wrapup_map=st.session_state.wrapup_map,
                         include_attributes=True
                     )
                     
                     if not df.empty:
                         # Filter for MISSED Only
                         # Condition: ConnectionStatus is NOT "Cevaplandı" or "Ulaşıldı" or "Bağlandı"
                         # OR strictly match "Kaçan/Cevapsız", "Ulaşılamadı", "Bağlanamadı"
                         # STRICT REQUIREMENT: Only Inbound
                         
                         missed_statuses = ["Kaçan/Cevapsız", "Ulaşılamadı", "Bağlanamadı", "Missed", "Unreachable"]
                         # Filter logic
                         if "ConnectionStatus" in df.columns and "Direction" in df.columns:
                             # Use isin for stricter matching if possible, or string contains for flexibility
                             # And Filter for Inbound
                             df_missed = df[
                                 (df["ConnectionStatus"].isin(missed_statuses)) & 
                                 (df["Direction"].astype(str).str.lower() == "inbound")
                             ]
                         else:
                             df_missed = pd.DataFrame() # Should not happen

                         if not df_missed.empty:
                             # Rename columns
                             col_map_internal = {
                                 "Direction": "col_direction",
                                 "Ani": "col_ani",
                                 "Dnis": "col_dnis",
                                 "Wrapup": "col_wrapup",
                                 "MediaType": "col_media",
                                 "Duration": "col_duration",
                                 "DisconnectType": "col_disconnect",
                                 "Alert": "col_alert",
                                 "HoldCount": "col_hold_count",
                                 "ConnectionStatus": "col_connection",
                                 "Start": "start_time",
                                 "End": "end_time",
                                 "Agent": "col_agent",
                                 "Username": "col_username",
                                 "Queue": "col_workgroup"
                             }
                             
                             final_cols = [k for k, v in col_map_internal.items() if v in selected_cols_keys]
                             df_filtered = df_missed[final_cols]
                             rename_final = {k: get_text(lang, col_map_internal[k]) for k in final_cols}
                             
                             final_df = df_filtered.rename(columns=rename_final)
                             
                             st.success(f"{len(final_df)} adet kaçan etkileşim bulundu.")
                             st.dataframe(final_df, use_container_width=True)
                             st.download_button(get_text(lang, "download_excel"), data=to_excel(final_df), file_name=f"missed_interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                         else:
                             st.warning("Seçilen kriterlere uygun kaçan çağrı/etkileşim bulunamadı.")
                     else:
                         st.warning(get_text(lang, "no_data"))

        # --- INTERACTION SEARCH ---
        if r_type == "interaction_search":
            st.info("Bu rapor, seçilen tarih aralığındaki çağrı, chat ve diğer etkileşimleri listeler. Çok uzun aralıklar seçmeyiniz.")
            
            # Dynamic Column Selection
            from src.lang import INTERACTION_COLUMNS
            default_cols = [c for c in INTERACTION_COLUMNS if c not in ["col_media", "col_wrapup"]] # Default subset
            
            # Allow user to customize columns if needed
            selected_cols_keys = st.multiselect("Sütunları Seçin / Select Columns", INTERACTION_COLUMNS, default=INTERACTION_COLUMNS, format_func=lambda x: get_text(lang, x))
            
            if st.button("Etkileşimleri Getir / Fetch Interactions", type="primary", use_container_width=True):
                 with st.spinner(get_text(lang, "fetching_data")):
                     start_date = datetime.combine(sd, st_) - timedelta(hours=3)
                     end_date = datetime.combine(ed, et) - timedelta(hours=3)
                     # Fetch data
                     api = GenesysAPI(st.session_state.api_client)
                     raw_convs = api.get_conversation_details(start_date, end_date)
                     
                     # Process with Wrapup Map
                     df = process_conversation_details(raw_convs, st.session_state.users_info, st.session_state.queues_map, st.session_state.wrapup_map)
                     
                     if not df.empty:
                         # Rename columns first to internal keys then to display names
                         col_map_internal = {
                             "Direction": "col_direction",
                             "Ani": "col_ani",
                             "Dnis": "col_dnis",
                             "Wrapup": "col_wrapup",
                             "MediaType": "col_media",
                             "Duration": "col_duration",
                             "DisconnectType": "col_disconnect",
                             "Alert": "col_alert",
                             "HoldCount": "col_hold_count",
                             "ConnectionStatus": "col_connection",
                             "Start": "start_time",
                             "End": "end_time",
                             "Agent": "col_agent",
                             "Username": "col_username",
                             "Queue": "col_workgroup"
                         }
                         
                         # Filter columns based on selection
                         final_cols = [k for k, v in col_map_internal.items() if v in selected_cols_keys]
                         df_filtered = df[final_cols]
                         
                         # Rename to Display Names
                         rename_final = {k: get_text(lang, col_map_internal[k]) for k in final_cols}
                         
                         df_filtered = apply_duration_formatting(df_filtered.copy())
                         final_df = df_filtered.rename(columns=rename_final)
                         st.dataframe(final_df, use_container_width=True)
                         st.download_button(get_text(lang, "download_excel"), data=to_excel(final_df), file_name=f"interactions_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                     else:
                         st.warning(get_text(lang, "no_data"))

        # --- STANDARD REPORTS ---
        elif r_type not in ["chat_detail", "missed_interactions"] and st.button(get_text(lang, "fetch_report"), type="primary", use_container_width=True):
            if not sel_mets: st.warning("Lütfen metrik seçiniz.")
            else:
                # Auto-save last used metrics
                st.session_state.last_metrics = sel_mets
                with st.spinner(get_text(lang, "fetching_data")):
                    api = GenesysAPI(st.session_state.api_client)
                    s_dt, e_dt = datetime.combine(sd, st_) - timedelta(hours=3), datetime.combine(ed, et) - timedelta(hours=3)
                    r_kind = "Agent" if r_type == "report_agent" else ("Workgroup" if r_type == "report_queue" else "Detailed")
                    g_by = ['userId'] if r_kind == "Agent" else (['queueId'] if r_kind == "Workgroup" else ['userId', 'queueId'])
                    f_type = 'user' if r_kind in ["Agent", "Detailed"] else 'queue'
                    
                    resp = api.get_analytics_conversations_aggregate(s_dt, e_dt, granularity=gran_opt[sel_gran], group_by=g_by, filter_type=f_type, filter_ids=sel_ids or None, metrics=sel_mets)
                    q_lookup = {v: k for k, v in st.session_state.queues_map.items()}
                    df = process_analytics_response(resp, st.session_state.users_info if is_agent else q_lookup, r_kind.lower(), queue_map=q_lookup)
                    
                    if df.empty and is_agent:
                        agent_data = []
                        for uid in (sel_ids or st.session_state.users_info.keys()):
                            u = st.session_state.users_info.get(uid, {})
                            row = {"Name": u.get('name', uid), "Username": u.get('username', "").split('@')[0], "Id": uid}
                            if r_kind == "Detailed": row.update({"WorkgroupName": "-", "AgentName": row["Name"], "Id": f"{uid}|-"})
                            agent_data.append(row)
                        df = pd.DataFrame(agent_data)
                    
                    if not df.empty:
                        p_keys = ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "col_staffed_time", "nNotResponding"]
                        if any(m in sel_mets for m in p_keys) and is_agent:
                            p_map = process_user_aggregates(api.get_user_aggregates(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())), st.session_state.get('presence_map'))
                            for pk in ["tMeal", "tMeeting", "tAvailable", "tBusy", "tAway", "tTraining", "tOnQueue", "StaffedTime", "nNotResponding"]:
                                df[pk if pk != "StaffedTime" and pk != "nNotResponding" else ("col_staffed_time" if pk == "StaffedTime" else "nNotResponding")] = df["Id"].apply(lambda x: p_map.get(x.split('|')[0] if '|' in x else x, {}).get(pk, 0))
                        
                        if any(m in sel_mets for m in ["col_login", "col_logout"]) and is_agent:
                            d_map = process_user_details(api.get_user_status_details(s_dt, e_dt, sel_ids or list(st.session_state.users_info.keys())))
                            if "col_login" in sel_mets: df["col_login"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Login", "N/A"))
                            if "col_logout" in sel_mets: df["col_logout"] = df["Id"].apply(lambda x: d_map.get(x.split('|')[0] if '|' in x else x, {}).get("Logout", "N/A"))

                        if do_fill and gran_opt[sel_gran] != "P1D": df = fill_interval_gaps(df, datetime.combine(sd, st_), datetime.combine(ed, et), gran_opt[sel_gran])
                        
                        base = (["AgentName", "Username", "WorkgroupName"] if r_kind == "Detailed" else (["Name", "Username"] if is_agent else ["Name"]))
                        if "Interval" in df.columns: base = ["Interval"] + base
                        for sm in sel_mets:
                            if sm not in df.columns: df[sm] = 0
                        # Avoid duplicates if AvgHandle is already in sel_mets
                        mets_to_show = [m for m in sel_mets if m in df.columns]
                        if "AvgHandle" in df.columns and "AvgHandle" not in mets_to_show:
                            mets_to_show.append("AvgHandle")
                        final_df = df[[c for c in base if c in df.columns] + mets_to_show]
                        
                        # Apply duration formatting
                        final_df = apply_duration_formatting(final_df)

                        rename = {"Interval": get_text(lang, "col_interval"), "AgentName": get_text(lang, "col_agent"), "Username": get_text(lang, "col_username"), "WorkgroupName": get_text(lang, "col_workgroup"), "Name": get_text(lang, "col_agent" if is_agent else "col_workgroup"), "AvgHandle": get_text(lang, "col_avg_handle"), "col_staffed_time": get_text(lang, "col_staffed_time"), "col_login": get_text(lang, "col_login"), "col_logout": get_text(lang, "col_logout")}
                        rename.update({m: get_text(lang, m) for m in sel_mets if m not in rename})
                        st.dataframe(final_df.rename(columns=rename), use_container_width=True)
                        st.download_button(get_text(lang, "download_excel"), data=to_excel(final_df.rename(columns=rename)), file_name=f"report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx")
                    else: st.warning(get_text(lang, "no_data"))

    elif st.session_state.page == "Kullanıcı Yönetimi" and role == "Admin":
        st.title("👤 Kullanıcı Yönetimi")
        
        with st.expander("➕ Yeni Kullanıcı Ekle", expanded=True):
            # Auto password generator
            col_gen1, col_gen2 = st.columns([3, 1])
            with col_gen2:
                if st.button("🔐 Şifre Oluştur", key="gen_pw_btn"):
                    st.session_state.generated_password = generate_password(12)
            
            generated_pw = st.session_state.get("generated_password", "")
            if generated_pw:
                col_gen1.success(f"Oluşturulan Şifre: **{generated_pw}**")
            
            with st.form("add_user_form"):
                new_un = st.text_input("Kullanıcı Adı")
                new_pw = st.text_input("Şifre", type="password", value=generated_pw, help="Otomatik şifre oluşturmak için yukarıdaki butonu kullanın")
                new_role = st.selectbox("Rol", ["Admin", "Manager", "Reports User", "Dashboard User"])
                
                from src.lang import ALL_METRICS
                new_mets = st.multiselect("İzinli Metrikler (Boş bırakılırsa hepsi seçilir)", ALL_METRICS, format_func=lambda x: get_text(lang, x))
                
                if st.form_submit_button("Ekle", use_container_width=True):
                    if new_un and new_pw:
                        success, msg = auth_manager.add_user(new_un, new_pw, new_role, new_mets)
                        if success: 
                            st.session_state.generated_password = ""  # Clear after use
                            st.success(msg)
                            st.rerun()
                        else: st.error(msg)
                    else: st.warning("Ad ve şifre gereklidir.")
        
        st.write("---")
        st.subheader("Mevcut Kullanıcılar")
        all_users = auth_manager.get_all_users()
        for uname, udata in all_users.items():
            col1, col2, col3, col4 = st.columns([2, 2, 4, 1])
            col1.write(f"**{uname}**")
            col2.write(f"Rol: {udata['role']}")
            col3.write(f"Metrikler: {', '.join(udata['metrics']) if udata['metrics'] else 'Hepsi'}")
            if uname != "admin": # Don't delete self
                if col4.button("Sil", key=f"del_user_{uname}"):
                    auth_manager.delete_user(uname)
                    st.rerun()
            st.write("---")

    else: # --- DASHBOARD ---
        if 'dashboard_config_loaded' not in st.session_state:
            config = load_dashboard_config()
            st.session_state.dashboard_layout, st.session_state.dashboard_cards = config.get("layout", 1), config.get("cards", [{"id": 0, "title": "", "queues": [], "size": "medium"}])
            st.session_state.dashboard_config_loaded = True
        
        st.title(get_text(lang, "menu_dashboard"))
        c_c1, c_c2, c_c3 = st.columns([1, 2, 1])
        if c_c1.button(get_text(lang, "add_group"), use_container_width=True):
            st.session_state.dashboard_cards.append({"id": max([c['id'] for c in st.session_state.dashboard_cards], default=-1)+1, "title": "", "queues": [], "size": "medium", "live_metrics": ["Waiting", "Interacting", "On Queue"], "daily_metrics": ["Offered", "Answered", "Abandoned", "Answer Rate"]})
            save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards); st.rerun()
        
        with c_c2:
            sc1, sc2 = st.columns([2, 3])
            lo = sc1.radio("Layout", [1, 2, 3], format_func=lambda x: f"Grid: {x}", index=st.session_state.dashboard_layout-1, horizontal=True, label_visibility="collapsed")
            if lo != st.session_state.dashboard_layout:
                st.session_state.dashboard_layout = lo; save_dashboard_config(lo, st.session_state.dashboard_cards); st.rerun()
            m_opts = ["Live", "Yesterday", "Date"]
            if 'dashboard_mode' not in st.session_state: st.session_state.dashboard_mode = "Live"
            st.session_state.dashboard_mode = sc2.radio("Mode", m_opts, format_func=lambda x: get_text(lang, f"mode_{x.lower()}"), index=m_opts.index(st.session_state.dashboard_mode), horizontal=True, label_visibility="collapsed")

        if c_c3:
            if st.session_state.dashboard_mode == "Date": st.session_state.dashboard_date = st.date_input("Date", datetime.today(), label_visibility="collapsed")
            elif st.session_state.dashboard_mode == "Live": auto_ref = st.toggle(get_text(lang, "auto_refresh"), value=True)
            if st.session_state.dashboard_mode == "Live" and auto_ref: st_autorefresh(interval=10000, key="data_refresh")

        # Available metric options
        LIVE_METRIC_OPTIONS = ["Waiting", "Interacting", "On Queue", "Available", "Busy", "Away", "Break", "Meal", "Meeting", "Training"]
        DAILY_METRIC_OPTIONS = ["Offered", "Answered", "Abandoned", "Answer Rate", "Service Level", "Avg Handle Time", "Avg Wait Time"]

        grid = st.columns(st.session_state.dashboard_layout)
        to_del = []
        for idx, card in enumerate(st.session_state.dashboard_cards):
            with grid[idx % st.session_state.dashboard_layout]:
                with st.container(border=True):
                    st.markdown(f"### {card['title'] or f'Grup #{card['id']+1}'}")
                    with st.expander(f"⚙️ Settings", expanded=False):
                        card['title'] = st.text_input("Title", value=card['title'], key=f"t_{card['id']}")
                        card['queues'] = st.multiselect("Queues", list(st.session_state.queues_map.keys()), default=card.get('queues', []), key=f"q_{card['id']}")
                        card['size'] = st.selectbox("Size", ["small", "medium", "large"], index=["small", "medium", "large"].index(card.get('size', 'medium')), key=f"s_{card['id']}")
                        
                        st.write("---")
                        st.caption("📡 Canlı Metrikler")
                        card['live_metrics'] = st.multiselect("Live Metrics", LIVE_METRIC_OPTIONS, default=card.get('live_metrics', ["Waiting", "Interacting", "On Queue"]), key=f"lm_{card['id']}")
                        
                        st.caption("📊 Günlük Metrikler")
                        card['daily_metrics'] = st.multiselect("Daily Metrics", DAILY_METRIC_OPTIONS, default=card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"]), key=f"dm_{card['id']}")
                        
                        if st.button("Delete", key=f"d_{card['id']}"): to_del.append(idx)
                        save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards)
                    
                    if not card.get('queues'): st.info("Select queues"); continue
                    
                    # Determine date range based on mode
                    if st.session_state.dashboard_mode == "Live":
                        # Use cached live data
                        obs_map, daily_map, _ = data_manager.get_data(card['queues'])
                        items_live = [obs_map.get(q) for q in card['queues'] if obs_map.get(q)]
                        items_daily = [daily_map.get(q) for q in card['queues'] if daily_map.get(q)]
                    else:
                        # Fetch historical data via API
                        items_live = []  # No live data for historical
                        
                        if st.session_state.dashboard_mode == "Yesterday":
                            target_date = datetime.today() - timedelta(days=1)
                        else:  # Date mode
                            target_date = datetime.combine(st.session_state.get('dashboard_date', datetime.today()), time(0, 0))
                        
                        # Calculate date range (full day in UTC)
                        start_dt = datetime.combine(target_date, time(0, 0)) - timedelta(hours=3)
                        end_dt = datetime.combine(target_date, time(23, 59, 59)) - timedelta(hours=3)
                        
                        # Fetch aggregate data for selected queues
                        queue_ids = [st.session_state.queues_map.get(q) for q in card['queues'] if st.session_state.queues_map.get(q)]
                        
                        items_daily = []
                        if queue_ids:
                            try:
                                api = GenesysAPI(st.session_state.api_client)
                                # Use Genesys standard aggregate query
                                interval = f"{start_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}/{end_dt.strftime('%Y-%m-%dT%H:%M:%S.000Z')}"
                                resp = api.get_queue_daily_stats(queue_ids, interval=interval)
                                
                                if resp and resp.get('results'):
                                    id_map = {v: k for k, v in st.session_state.queues_map.items()}
                                    from src.processor import process_daily_stats
                                    daily_data = process_daily_stats(resp, id_map)
                                    items_daily = [daily_data.get(q) for q in card['queues'] if daily_data.get(q)]
                            except Exception as e:
                                st.warning(f"Veri çekilemedi: {e}")
                    
                    # Calculate aggregates
                    n_q = len(items_live) or 1
                    n_s = len(card['queues']) or 1
                    off = sum(d['Offered'] for d in items_daily)
                    ans = sum(d['Answered'] for d in items_daily)
                    abn = sum(d['Abandoned'] for d in items_daily)
                    s_n = sum(d.get('SL_Numerator', 0) for d in items_daily)
                    s_d = sum(d.get('SL_Denominator', 0) for d in items_daily)
                    sl = (s_n / s_d * 100) if s_d > 0 else 0
                    avg_handle = sum(d.get('AvgHandle', 0) for d in items_daily) / len(items_daily) if items_daily else 0
                    avg_wait = sum(d.get('AvgWait', 0) for d in items_daily) / len(items_daily) if items_daily else 0
                    
                    # Live metrics mapping
                    live_values = {
                        "Waiting": sum(d['Waiting'] for d in items_live) if items_live else 0,
                        "Interacting": round(sum(d['Interacting'] for d in items_live)/n_q) if items_live else 0,
                        "On Queue": round(sum(d['Presences'].get('On Queue', 0) for d in items_live)/n_s) if items_live else 0,
                        "Available": round(sum(d['Presences'].get('Available', 0) for d in items_live)/n_s) if items_live else 0,
                        "Busy": round(sum(d['Presences'].get('Busy', 0) for d in items_live)/n_s) if items_live else 0,
                        "Away": round(sum(d['Presences'].get('Away', 0) for d in items_live)/n_s) if items_live else 0,
                        "Break": round(sum(d['Presences'].get('Break', 0) for d in items_live)/n_s) if items_live else 0,
                        "Meal": round(sum(d['Presences'].get('Meal', 0) for d in items_live)/n_s) if items_live else 0,
                        "Meeting": round(sum(d['Presences'].get('Meeting', 0) for d in items_live)/n_s) if items_live else 0,
                        "Training": round(sum(d['Presences'].get('Training', 0) for d in items_live)/n_s) if items_live else 0,
                    }
                    
                    live_labels = {"Waiting": get_text(lang, "waiting"), "Interacting": get_text(lang, "interacting"), "On Queue": get_text(lang, "on_queue_agents"), "Available": get_text(lang, "available_agents"), "Busy": "Meşgul", "Away": "Uzakta", "Break": "Mola", "Meal": "Yemek", "Meeting": "Toplantı", "Training": "Eğitim"}
                    
                    # Daily metrics mapping
                    daily_values = {
                        "Offered": off,
                        "Answered": ans,
                        "Abandoned": abn,
                        "Answer Rate": f"%{(ans/off*100) if off>0 else 0:.1f}",
                        "Service Level": f"%{sl:.1f}",
                        "Avg Handle Time": f"{avg_handle/60:.1f}m" if avg_handle else "0",
                        "Avg Wait Time": f"{avg_wait:.0f}s" if avg_wait else "0",
                    }
                    
                    daily_labels = {"Offered": get_text(lang, "offered"), "Answered": get_text(lang, "answered"), "Abandoned": get_text(lang, "abandoned"), "Answer Rate": get_text(lang, "answer_rate"), "Service Level": get_text(lang, "avg_service_level"), "Avg Handle Time": "Ort. İşlem", "Avg Wait Time": "Ort. Bekleme"}
                    
                    if st.session_state.dashboard_mode == "Live":
                        # Show selected live metrics
                        sel_live = card.get('live_metrics', ["Waiting", "Interacting", "On Queue"])
                        if sel_live:
                            m = st.columns(min(len(sel_live), 5))
                            for i, metric in enumerate(sel_live[:5]):
                                m[i].metric(live_labels.get(metric, metric), live_values.get(metric, 0))
                        
                        # Show daily summary below live (Today's stats)
                        st.divider()
                        st.caption(f"📅 Bugünün Özeti")
                        sel_daily = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                        if sel_daily:
                            dm = st.columns(min(len(sel_daily), 4))
                            for i, metric in enumerate(sel_daily[:4]):
                                dm[i].metric(daily_labels.get(metric, metric), daily_values.get(metric, 0))
                        
                        # Always show gauge for Service Level at the bottom
                        gauge_size = 180 if card['size'] == 'small' else (200 if card['size'] == 'medium' else 250)
                        st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), gauge_size), use_container_width=True, key=f"g_{card['id']}")
                    
                    else:
                        # Historical mode (Yesterday/Date) - show daily stats with gauge
                        sel_daily = card.get('daily_metrics', ["Offered", "Answered", "Abandoned", "Answer Rate"])
                        
                        # Show daily metrics first for ALL sizes
                        st.caption(f"📅 {get_text(lang, 'daily_stat')}")
                        dm = st.columns(min(len(sel_daily), 4))
                        for i, metric in enumerate(sel_daily[:4]):
                            dm[i].metric(daily_labels.get(metric, metric), daily_values.get(metric, 0))
                        
                        # Then show gauge
                        gauge_size = 180 if card['size'] == 'small' else (250 if card['size'] == 'medium' else 300)
                        st.plotly_chart(create_gauge_chart(sl, get_text(lang, "avg_service_level"), gauge_size), use_container_width=True, key=f"g_{card['id']}")

        if to_del:
            for i in sorted(to_del, reverse=True): del st.session_state.dashboard_cards[i]
            save_dashboard_config(st.session_state.dashboard_layout, st.session_state.dashboard_cards); st.rerun()
