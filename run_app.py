import streamlit.web.cli as stcli
import os
import sys
import signal
import time

def resolve_path(path):
    """Get absolute path to resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, path)

PID_FILE = ".app.pid"

def check_single_instance():
    """Ensures only one instance of the app is running by killing the previous one."""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, "r") as f:
                old_pid = int(f.read().strip())
            # Check if process exists and kill it
            try:
                os.kill(old_pid, signal.SIGTERM)
                time.sleep(2) # Wait for it to shutdown and free the port
            except ProcessLookupError:
                pass
        except Exception:
            pass
    
    # We will write the new PID in app.py or here? 
    # Better to write it here so we know the runner's PID (which usually persists or spawns streamlit)
    with open(PID_FILE, "w") as f:
        f.write(str(os.getpid()))

if __name__ == "__main__":
    check_single_instance()
    app_path = resolve_path("app.py")
    
    # Simulate: streamlit run app.py
    sys.argv = [
        "streamlit",
        "run",
        app_path,
        "--server.port=8501",
        "--server.address=localhost",
        "--server.headless=true",
        "--global.developmentMode=false",
    ]
    
    # Launch Streamlit
    sys.exit(stcli.main())
