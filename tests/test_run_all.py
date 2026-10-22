import sys
import threading

import pytest

from scripts import run_all

PY = sys.executable


def test_output_is_prefixed_and_stop_event_terminates(capsys):
    services = [
        ("alpha", [PY, "-u", "-c", "import time; print('hello from alpha'); time.sleep(60)"]),
        ("beta", [PY, "-u", "-c", "import time; print('hello from beta'); time.sleep(60)"]),
    ]
    stop = threading.Event()
    threading.Timer(2.0, stop.set).start()
    code = run_all.run_services(services, stop, poll=0.1)
    out = capsys.readouterr().out
    assert code == 0
    assert "[alpha] hello from alpha" in out
    assert "[beta] hello from beta" in out


def test_when_one_service_dies_everything_stops(capsys):
    services = [
        ("steady", [PY, "-u", "-c", "import time; time.sleep(60)"]),
        ("crashy", [PY, "-u", "-c", "import sys; print('boom'); sys.exit(3)"]),
    ]
    code = run_all.run_services(services, poll=0.1)
    out = capsys.readouterr().out
    assert code == 3
    assert "[crashy] boom" in out
    assert "crashy exited with code 3" in out


@pytest.mark.skipif(sys.platform == "win32", reason="terminate() is a hard kill on Windows")
def test_services_get_to_finish_their_shutdown_work(capsys):
    script = (
        "import signal, sys, time\n"
        "signal.signal(signal.SIGTERM, signal.default_int_handler)\n"
        "print('ready', flush=True)\n"
        "try:\n"
        "    time.sleep(60)\n"
        "except KeyboardInterrupt:\n"
        "    print('flushed final batch', flush=True)\n"
    )
    stop = threading.Event()
    threading.Timer(2.0, stop.set).start()
    run_all.run_services([("worker", [PY, "-u", "-c", script])], stop, poll=0.1)
    assert "[worker] flushed final batch" in capsys.readouterr().out
