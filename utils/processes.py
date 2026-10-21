import multiprocessing


def use_forkserver():
    """Start worker processes from a fork server instead of forking this process.

    A forked worker inherits the SIGTERM handler from exit_cleanly_on_sigterm().
    Pool.terminate() sends SIGTERM and joins every worker; when the signal lands
    while a worker runs a weakref finalizer (a Manager proxy's decref), the
    SystemExit is printed and ignored, the worker lives on, blocks on the
    pool's task queue, and the join never returns (#116). Forking from a
    background thread while other threads run also copies the locks those
    threads hold (logging handlers, queues), which nothing releases in the child.

    The fork server is a fresh single-threaded process: workers forked from it
    hold no copied locks and keep the default SIGTERM action. Call this at
    startup, before any pool exists.
    """
    multiprocessing.set_start_method("forkserver", force=True)
