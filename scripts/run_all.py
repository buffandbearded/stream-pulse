"""Start the whole pipeline from one terminal.

    python scripts/run_all.py              # Redpanda + producer + consumer + dashboard
    python scripts/run_all.py --no-infra   # skip the docker step (Redpanda already running)

Output of every service is prefixed with its name. Ctrl+C stops everything.
"""
import argparse
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ["docker", "compose", "-f", "infra/docker-compose.yml"]
TOPICS = [("wiki.recentchange", 3), ("wiki.recentchange.dlq", 1)]

SERVICES = [
    ("producer", [sys.executable, "-u", "src/producer.py"]),
    ("consumer", [sys.executable, "-u", "-m", "src.consumer"]),
    ("dashboard", [sys.executable, "-u", "-m", "streamlit", "run", "app/dashboard.py"]),
]


def sh(cmd):
    return subprocess.run(cmd, cwd=ROOT, capture_output=True, text=True)


def start_infra() -> None:
    print("[infra] starting Redpanda with docker compose ...", flush=True)
    result = sh(COMPOSE + ["up", "-d"])
    if result.returncode != 0:
        sys.exit(f"docker compose failed (is Docker Desktop running?):\n{result.stderr}")
    deadline = time.time() + 90
    while time.time() < deadline:
        if sh(["docker", "exec", "redpanda", "rpk", "cluster", "info"]).returncode == 0:
            break
        time.sleep(2)
    else:
        sys.exit("Redpanda did not become ready within 90 seconds")
    for topic, partitions in TOPICS:
        result = sh(["docker", "exec", "redpanda", "rpk", "topic", "create", topic, "-p", str(partitions)])
        output = result.stdout + result.stderr
        state = "created" if result.returncode == 0 else (
            "already exists" if "ALREADY_EXISTS" in output else f"WARNING: {output.strip()[:120]}")
        print(f"[infra] topic {topic}: {state}", flush=True)


def _pump(name: str, proc: subprocess.Popen) -> None:
    for line in proc.stdout:
        print(f"[{name}] {line.rstrip()}", flush=True)


def _shutdown(procs: list) -> None:
    for _, proc, _ in reversed(procs):
        if proc.poll() is None:
            proc.terminate()
    deadline = time.time() + 10
    for _, proc, _ in procs:
        try:
            proc.wait(timeout=max(0.1, deadline - time.time()))
        except subprocess.TimeoutExpired:
            proc.kill()


def run_services(services, stop: threading.Event | None = None, poll: float = 0.5) -> int:
    """Run services until Ctrl+C, `stop` is set, or any service exits. Returns an exit code."""
    env = {**os.environ, "PYTHONUNBUFFERED": "1", "PYTHONIOENCODING": "utf-8"}
    procs = []
    for name, cmd in services:
        proc = subprocess.Popen(cmd, cwd=ROOT, env=env, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                text=True, encoding="utf-8", errors="replace")
        thread = threading.Thread(target=_pump, args=(name, proc), daemon=True)
        thread.start()
        procs.append((name, proc, thread))

    code = 0
    try:
        running = True
        while running and not (stop and stop.is_set()):
            for name, proc, _ in procs:
                rc = proc.poll()
                if rc is not None:
                    print(f"[run_all] {name} exited with code {rc}; stopping everything", flush=True)
                    code = rc
                    running = False
                    break
            time.sleep(poll)
    except KeyboardInterrupt:
        print("\n[run_all] Ctrl+C received, stopping ...", flush=True)
    finally:
        _shutdown(procs)
        for _, _, thread in procs:
            thread.join(timeout=3)  # let the services' final output reach the terminal
    return code


def main() -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--no-infra", action="store_true", help="skip starting Redpanda via docker compose")
    args = parser.parse_args()
    if not args.no_infra:
        start_infra()
    print("[run_all] starting producer, consumer and dashboard.", flush=True)
    print("[run_all] dashboard: http://localhost:8501   (Ctrl+C stops everything)", flush=True)
    sys.exit(run_services(SERVICES))


if __name__ == "__main__":
    main()
