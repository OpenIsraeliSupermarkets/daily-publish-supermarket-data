import signal


def _exit_on_sigterm(signum, frame):
    raise SystemExit(128 + signum)


def exit_cleanly_on_sigterm():
    """Turn SIGTERM into SystemExit.

    `docker stop` (and so every redeploy) sends SIGTERM, which by default ends
    the process without unwinding the stack. Raising SystemExit instead lets
    context managers run, so CacheManager writes the push progress (#108).
    """
    signal.signal(signal.SIGTERM, _exit_on_sigterm)
