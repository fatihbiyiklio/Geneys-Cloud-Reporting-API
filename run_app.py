import os
import sys
import signal
import time
import platform
import subprocess
import socket
import streamlit.web.cli as stcli

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
CHILD_FLAG = "--streamlit-child"

_lock_handle = None


def _env_flag(name, default="0"):
    return os.environ.get(name, default).strip().lower() in ("1", "true", "yes", "on")


def _is_windows_admin():
    try:
        import ctypes
        return bool(ctypes.windll.shell32.IsUserAnAdmin())
    except Exception:
        return False


def _run_sc_command(*args):
    try:
        proc = subprocess.run(
            ["sc.exe", *args],
            capture_output=True,
            text=True,
            check=False,
        )
        output = ((proc.stdout or "") + "\n" + (proc.stderr or "")).strip()
        return proc.returncode == 0, output
    except Exception as e:
        return False, str(e)


def ensure_windows_service_registration(log_func):
    """Register this app as a Windows service (startup: automatic)."""
    if not platform.system().lower().startswith("win"):
        return
    if not _env_flag("GENESYS_WINDOWS_SERVICE_AUTO_INSTALL", "1"):
        return

    service_name = os.environ.get("GENESYS_WINDOWS_SERVICE_NAME", "GenesysReporting").strip() or "GenesysReporting"
    display_name = os.environ.get("GENESYS_WINDOWS_SERVICE_DISPLAY_NAME", "Genesys Cloud Reporting").strip() or "Genesys Cloud Reporting"
    description = os.environ.get(
        "GENESYS_WINDOWS_SERVICE_DESCRIPTION",
        "Genesys Cloud Reporting Streamlit service",
    ).strip() or "Genesys Cloud Reporting Streamlit service"
    start_mode = os.environ.get("GENESYS_WINDOWS_SERVICE_START_MODE", "auto").strip().lower() or "auto"
    if start_mode not in ("auto", "demand", "disabled", "delayed-auto"):
        start_mode = "auto"

    exists, _ = _run_sc_command("query", service_name)
    if exists:
        # Keep startup mode in sync.
        _run_sc_command("config", service_name, "start=", start_mode)
        return

    if not _is_windows_admin():
        log_func("Windows service auto-install skipped: run once as Administrator.")
        return

    if getattr(sys, "frozen", False):
        bin_path = f"\"{sys.executable}\""
    else:
        bin_path = f"\"{sys.executable}\" \"{os.path.abspath(__file__)}\""

    ok, out = _run_sc_command(
        "create",
        service_name,
        "binPath=",
        bin_path,
        "start=",
        start_mode,
        "DisplayName=",
        display_name,
    )
    if not ok:
        log_func(f"Windows service create failed for '{service_name}': {out}")
        return

    _run_sc_command("description", service_name, description)
    log_func(f"Windows service registered: {service_name} (start={start_mode})")

def run_streamlit_child():
    """Run Streamlit in child mode inside this process."""
    if len(sys.argv) < 3:
        print("Missing app path for child mode.")
        sys.exit(2)
    app_path = sys.argv[2]
    extra_args = sys.argv[3:]
    argv = ["streamlit", "run", app_path] + extra_args
    old_argv = sys.argv[:]
    sys.argv = argv
    try:
        stcli.main()
        return 0
    except SystemExit as e:
        return e.code or 0
    finally:
        sys.argv = old_argv

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

    # Child process mode: do not acquire single-instance lock.
    if len(sys.argv) > 1 and sys.argv[1] == CHILD_FLAG:
        sys.exit(run_streamlit_child())

    acquire_single_instance_lock()
    app_path = resolve_path("app.py")
    force_port_cleanup = os.environ.get("GENESYS_FORCE_PORT_CLEANUP", "0").strip().lower() in ("1", "true", "yes", "on")

    # Log file for debugging startup issues (especially on Windows)
    log_path = os.path.join(os.getcwd(), "app_startup.log")
    def log(msg):
        try:
            with open(log_path, "a", encoding="utf-8") as f:
                f.write(msg + "\n")
        except Exception:
            pass

    ensure_windows_service_registration(log)
    
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
            if not force_port_cleanup:
                try:
                    cmdline = " ".join(proc.cmdline()).lower()
                except (psutil.AccessDenied, psutil.ZombieProcess, psutil.NoSuchProcess):
                    cmdline = ""
                # Only terminate likely app-related processes by default.
                is_app_proc = (
                    "streamlit" in cmdline
                    and ("app.py" in cmdline or "run_app.py" in cmdline or CHILD_FLAG in cmdline)
                )
                if not is_app_proc:
                    return False
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

    def _resolve_server_address():
        env_address = os.environ.get("GENESYS_SERVER_ADDRESS", "").strip()
        if env_address:
            return env_address
        if os.path.exists("/.dockerenv"):
            return "0.0.0.0"
        return "localhost"

    # Launch Streamlit in a loop for auto-restart
    print("🚀 Starting Genesys Reporting App...")
    
    restart_count = 0
    last_restart = time.time()
    restart_exit_code = int(os.environ.get("GENESYS_RESTART_EXIT_CODE", "42"))
    restart_exit_codes = {1, restart_exit_code}
    final_exit_code = 0

    def run_streamlit():
        server_address = _resolve_server_address()
        common_args = [
            "--server.port=8501",
            f"--server.address={server_address}",
            "--server.headless=true",
            "--global.developmentMode=false",
        ]
        if getattr(sys, "frozen", False):
            argv = [sys.executable, CHILD_FLAG, app_path] + common_args
        else:
            argv = [sys.executable, os.path.abspath(__file__), CHILD_FLAG, app_path] + common_args
        proc = subprocess.Popen(argv)
        return proc.wait()

    while True:
        try:
            # Force Kill: Ensure 8501 is free before starting
            kill_proc_on_port(8501)
            if _is_port_busy(8501):
                msg = "Port 8501 is in use by a non-app process. Refusing to terminate unrelated process."
                print(f"❌ {msg}")
                if not force_port_cleanup:
                    print("Set GENESYS_FORCE_PORT_CLEANUP=1 only if you want to force-terminate any process on this port.")
                log(msg)
                final_exit_code = 1
                break
            
            log(f"Starting streamlit: {app_path}")
            exit_code = run_streamlit()
            
            if exit_code == 0:
                print("🛑 Application stopped gracefully (Exit Code 0).")
                log("Exit code 0. Wrapper stopping.")
                final_exit_code = 0
                break

            restart_count += 1
            now = time.time()
            if now - last_restart > 60:
                restart_count = 1
            last_restart = now
            log(f"Exit code {exit_code}. Restart count: {restart_count}")
            if restart_count >= 8:
                print("❌ Too many restarts. Exiting.")
                log("Too many restarts. Exiting.")
                final_exit_code = exit_code if exit_code not in restart_exit_codes else 1
                break

            if exit_code in restart_exit_codes:
                print(f"♻️ Restart requested (Exit Code {exit_code}). Restarting in 2 seconds...")
                time.sleep(2)
            else:
                print(f"⚠️ Unexpected exit code {exit_code}. Restarting in 5 seconds...")
                time.sleep(5)
        except KeyboardInterrupt:
            print("\n👋 Manual interruption. Exiting...")
            final_exit_code = 130
            break
        except Exception as e:
            print(f"❌ Fatal Error in wrapper: {e}")
            log(f"Fatal Error in wrapper: {e}")
            time.sleep(5)
            
    sys.exit(final_exit_code)
