import streamlit.web.cli as stcli
import os
import sys
import signal
import time
import platform

def resolve_path(path):
    """Get absolute path to resource, works for dev and for PyInstaller."""
    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, path)

PID_FILE = ".app.pid"
LOCK_FILE = ".app.lock"

_lock_handle = None

def acquire_single_instance_lock():
    """Prevent multiple instances from running at the same time."""
    global _lock_handle
    try:
        _lock_handle = open(LOCK_FILE, "w")
        if platform.system().lower().startswith("win"):
            import msvcrt
            try:
                msvcrt.locking(_lock_handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                print("Another instance is already running.")
                sys.exit(0)
        else:
            import fcntl
            try:
                fcntl.flock(_lock_handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError:
                print("Another instance is already running.")
                sys.exit(0)
    except Exception:
        # If lock fails for any reason, do not start another instance
        print("Another instance is already running.")
        sys.exit(0)

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
    import psutil

    acquire_single_instance_lock()
    app_path = resolve_path("app.py")

    # Log file for debugging startup issues (especially on Windows)
    log_path = os.path.join(os.path.dirname(app_path), "app_startup.log")
    def log(msg):
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
    
    def kill_proc_on_port(port):
        """Finds and kills any process using the specified port."""
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                for conns in proc.connections(kind='inet'):
                    if conns.laddr.port == port:
                        print(f"🔪 Killing process {proc.info['name']} (PID: {proc.info['pid']}) on port {port}...")
                        proc.terminate()
                        try:
                            proc.wait(timeout=3)
                        except psutil.TimeoutExpired:
                            proc.kill()
                        return True
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        return False

    # Launch Streamlit in a loop for auto-restart
    print("🚀 Starting Genesys Reporting App...")
    
    restart_count = 0
    last_restart = time.time()
    while True:
        try:
            # Force Kill: Ensure 8501 is free before starting
            kill_proc_on_port(8501)
            
            # subprocess.call returns the exit code
            # We use sys.executable to ensure we use the same Python interpreter
            cmd = [sys.executable, "-m", "streamlit", "run", app_path, 
                  "--server.port=8501", "--server.address=localhost", 
                  "--server.headless=true", "--global.developmentMode=false"]
            
            # Using stcli directly in same process allows simple pyinstaller build but harder restart
            # Switching to subprocess for robust isolation and restart capability
            import subprocess
            log(f"Starting streamlit: {' '.join(cmd)}")
            exit_code = subprocess.call(cmd)
            
            if exit_code == 0:
                print("🛑 Application stopped gracefully (Exit Code 0).")
                break
            else:
                restart_count += 1
                now = time.time()
                if now - last_restart > 60:
                    restart_count = 1
                last_restart = now
                log(f"Exit code {exit_code}. Restart count: {restart_count}")
                if restart_count >= 5:
                    print("❌ Too many restarts. Exiting.")
                    log("Too many restarts. Exiting.")
                    break
                print(f"⚠️ Application exited with code {exit_code}. Restarting in 2 seconds...")
                time.sleep(2)
        except KeyboardInterrupt:
            print("\n👋 Manual interruption. Exiting...")
            break
        except Exception as e:
            print(f"❌ Fatal Error in wrapper: {e}")
            log(f"Fatal Error in wrapper: {e}")
            time.sleep(5)
            
    sys.exit(0)
