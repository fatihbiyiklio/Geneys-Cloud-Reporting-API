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

    # Try to write the new PID, handle PermissionError (common on Windows if file is locked)
    try:
        # If file exists, try to remove it first to break potential locks
        if os.path.exists(PID_FILE):
            try:
                os.remove(PID_FILE)
            except:
                pass
        
        with open(PID_FILE, "w") as f:
            f.write(str(os.getpid()))
    except PermissionError:
        print(f"Warning: Could not write {PID_FILE}. Another instance might be locking it.")
    except Exception as e:
        print(f"Warning: PID file error: {e}")

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
