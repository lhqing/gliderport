class JobListener:
    """
    Job Listener detects new jobs and upload them to GCS if needed.

    Listen to a directory for new job files, start data uploader and return job_id when file is ready on GCS.
    """

    def __init__(self):
        pass


class WorkerManager:
    """Manage sky spot-VM workers."""

    def __init__(self):
        pass


class GliderPort:
    """
    Sky manager run on-prime.

    Determine how to arrange job listener and workers,
    how to distribute jobs to workers,
    how to control the speed,
    and how to decide when to stop.
    """

    def __init__(self, local_job_dir, sky_yaml_template, n_uploader=1, n_worker=16):
        return

    def on(self):
        """Run on-prime."""
        return
