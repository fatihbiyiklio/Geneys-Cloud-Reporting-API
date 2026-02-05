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
            except:
                pass
        except:
            pass
    
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
    
    # Launch Streamlit in a loop for auto-restart
    print("🚀 Starting Genesys Reporting App...")
    
    while True:
        try:
            # subprocess.call returns the exit code
            # We use sys.executable to ensure we use the same Python interpreter
            cmd = [sys.executable, "-m", "streamlit", "run", app_path, 
                  "--server.port=8501", "--server.address=localhost", 
                  "--server.headless=true", "--global.developmentMode=false"]
            
            # Using stcli directly in same process allows simple pyinstaller build but harder restart
            # Switching to subprocess for robust isolation and restart capability
            import subprocess
            exit_code = subprocess.call(cmd)
            
            if exit_code == 0:
                print("🛑 Application stopped gracefully (Exit Code 0).")
                break
            else:
                print(f"⚠️ Application exited with code {exit_code}. Restarting in 1 second...")
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n👋 Manual interruption. Exiting...")
            break
        except Exception as e:
            print(f"❌ Fatal Error in wrapper: {e}")
            time.sleep(5)
            
    sys.exit(0)
