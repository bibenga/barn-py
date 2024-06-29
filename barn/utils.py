
def set_proc_title(name: str) -> None:
    try:
        from setproctitle import setproctitle
    except ImportError:
        pass
    else:
        setproctitle(name)
