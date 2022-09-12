try:
    from importlib.metadata import version
except ImportError:  # for Python<3.8
    from importlib_metadata import version

__version__ = version("gliderport")

try:
    import sky

    code_path = f"{sky.__path__[0]}/backends/backend_utils.py"
    with open(code_path) as f:
        code = f.read()
        if code.find("DEFAULT_TASK_CPU_DEMAND = 0.5") == -1:
            flag = False
        else:
            flag = True

    if flag:
        with open(code_path, "w") as f:
            print("Modify code in", code_path)
            f.write(code.replace("DEFAULT_TASK_CPU_DEMAND = 0.5", "DEFAULT_TASK_CPU_DEMAND = 0.2"))
        from importlib import reload  # Python 3.4+

        reload(sky)
        from sky.backends.backend_utils import DEFAULT_TASK_CPU_DEMAND

        print("DEFAULT_TASK_CPU_DEMAND = 0.2 now")
except (ImportError, FileNotFoundError):
    pass
