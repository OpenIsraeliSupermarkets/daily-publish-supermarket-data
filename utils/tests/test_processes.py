"""Worker pools start from a fork server, not by forking this process (#116)."""

import multiprocessing
import signal
import threading

import pytest

from utils import exit_cleanly_on_sigterm, use_forkserver

# held by a background thread while the pool starts, like a logging handler's lock
HELD_LOCK = threading.Lock()


def _sigterm_handler(_):
    return str(signal.getsignal(signal.SIGTERM))


def _acquire_held_lock(_):
    acquired = HELD_LOCK.acquire(timeout=5)
    if acquired:
        HELD_LOCK.release()
    return acquired


@pytest.fixture(autouse=True)
def restore_process_state():
    previous_handler = signal.getsignal(signal.SIGTERM)
    previous_method = multiprocessing.get_start_method(allow_none=True)
    yield
    signal.signal(signal.SIGTERM, previous_handler)
    multiprocessing.set_start_method(previous_method, force=True)


def test_start_method_is_forkserver():
    use_forkserver()
    assert multiprocessing.get_start_method() == "forkserver"


def test_workers_keep_the_default_sigterm_action():
    """Pool.terminate() has to kill workers, not raise SystemExit in them."""
    exit_cleanly_on_sigterm()
    use_forkserver()
    with multiprocessing.Pool(2) as pool:
        handlers = pool.map(_sigterm_handler, range(2))
    assert handlers == [str(signal.SIG_DFL)] * 2


def test_workers_do_not_inherit_locks_held_by_other_threads():
    """A pool started from a thread while another thread holds a lock."""
    use_forkserver()
    holding = threading.Event()
    release = threading.Event()

    def hold():
        with HELD_LOCK:
            holding.set()
            release.wait()

    holder = threading.Thread(target=hold)
    holder.start()
    holding.wait()
    results = []

    def run_pool():
        with multiprocessing.Pool(2) as pool:
            results.extend(pool.map(_acquire_held_lock, range(2)))

    runner = threading.Thread(target=run_pool)
    runner.start()
    runner.join(timeout=60)
    release.set()
    holder.join()
    assert not runner.is_alive()
    assert results == [True, True]
