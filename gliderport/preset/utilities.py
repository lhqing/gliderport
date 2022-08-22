def _get_cpu_from_instance_name(instance_name):
    return int(instance_name.split("-")[-1])
