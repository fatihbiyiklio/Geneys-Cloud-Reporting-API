import os
import sys
import signal
import time
import platform
import subprocess
import socket

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
        try:
            _lock_handle.write(str(os.getpid()))
            _lock_handle.flush()
        except Exception:
            pass
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

    # Simple self-test hook for CI/debugging
    if os.environ.get("GENESYS_SELF_TEST") == "1":
        print("SELF_TEST_OK")
        sys.exit(0)

    acquire_single_instance_lock()
    app_path = resolve_path("app.py")

    # Log file for debugging startup issues (especially on Windows)
    log_path = os.path.join(os.getcwd(), "app_startup.log")
    def log(msg):
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass
    
    def _is_port_busy(port, host="127.0.0.1", timeout=0.25):
        """Fast port probe to skip expensive process scans when port is already free."""
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(timeout)
        try:
            return sock.connect_ex((host, port)) == 0
        except OSError:
            return False
        finally:
            sock.close()

    def _terminate_pid(pid):
        try:
            proc = psutil.Process(pid)
            name = proc.name()
            print(f"🔪 Killing process {name} (PID: {pid})...")
            proc.terminate()
            try:
                proc.wait(timeout=3)
            except psutil.TimeoutExpired:
                proc.kill()
            return True
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
            return False

    def kill_proc_on_port(port):
        """Finds and kills any process using the specified port."""
        if not _is_port_busy(port):
            return False

        candidate_pids = set()
        try:
            for conn in psutil.net_connections(kind='inet'):
                laddr = getattr(conn, "laddr", None)
                lport = getattr(laddr, "port", None) if laddr else None
                if lport == port and conn.pid and conn.pid != os.getpid():
                    candidate_pids.add(conn.pid)
        except (psutil.AccessDenied, PermissionError):
            pass

        for pid in candidate_pids:
            if _terminate_pid(pid):
                return True

        # Fallback path when global net_connections is restricted.
        for proc in psutil.process_iter(['pid', 'name']):
            try:
                for conn in proc.net_connections(kind='inet'):
                    laddr = getattr(conn, "laddr", None)
                    lport = getattr(laddr, "port", None) if laddr else None
                    if lport == port and proc.info['pid'] != os.getpid():
                        return _terminate_pid(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        return False

    # Launch Streamlit in a loop for auto-restart
    print("🚀 Starting Genesys Reporting App...")
    
    restart_count = 0
    last_restart = time.time()
    def run_streamlit():
        argv = [
            sys.executable, "-m", "streamlit", "run", app_path,
            "--server.port=8501", "--server.address=localhost",
            "--server.headless=true", "--global.developmentMode=false"
        ]
        proc = subprocess.Popen(argv)
        return proc.wait()

    while True:
        try:
            # Force Kill: Ensure 8501 is free before starting
            kill_proc_on_port(8501)
            
            log(f"Starting streamlit: {app_path}")
            exit_code = run_streamlit()
            
            restart_count += 1
            now = time.time()
            if now - last_restart > 60:
                restart_count = 1
            last_restart = now
            log(f"Exit code {exit_code}. Restart count: {restart_count}")
            if restart_count >= 8:
                print("❌ Too many restarts. Exiting.")
                log("Too many restarts. Exiting.")
                break
            if exit_code == 0:
                print("ℹ️ Application exited with code 0. Restarting in 2 seconds...")
            else:
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
