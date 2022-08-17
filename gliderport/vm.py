import yaml


class Job:
    """Job created from a config file."""

    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self._read_config()

    def _read_config(self):
        with open(self.config_file) as fp:
            config = yaml.load(fp, Loader=yaml.FullLoader)

        assert "Input" in config, "Input is not defined in config"
        assert "Output" in config, "Output is not defined in config"
        assert "Run" in config, "Worker is not defined in config"

        if "delete_input" in config:
            self._delete_input_from_gcs_flag = config["delete_input"]
        else:
            self._delete_input_from_gcs_flag = False

        return config

    def _move_input_to_pd(self):
        pass

    def _move_output_to_gcs(self):
        pass

    def _run(self):
        pass

    def _validate_input_transfer(self):
        pass

    def _validate_run(self):
        pass

    def _validate_output_transfer(self):
        pass

    def _delete_input_from_pd(self):
        pass

    def _delete_input_from_gcs(self):
        pass

    def run(self):
        """Run the job."""
        self._move_input_to_pd()
        self._validate_input_transfer()

        self._run()
        self._validate_run()

        self._move_output_to_gcs()
        self._validate_output_transfer()

        self._delete_input_from_pd()
        if self._delete_input_from_gcs_flag:
            self._delete_input_from_gcs()
        return True


class Worker:
    """Worker run inside VM, identify jobs to run by itself, stop when there is no job to run for a while."""

    def __init__(self, job_bucket_dir):
        self.job_bucket_dir = job_bucket_dir
        self.job_configs = []

        self.run()
        return

    def _get_jobs(self):
        pass

    def _run_job(self):
        # Initialize job object
        for config_paths in self.job_configs:
            job = Job(config_paths)
            job.run()

    def _validate_job_status(self):
        """For those succeeded jobs, delete them from job bucket."""

    def run(self):
        """
        Run the jobs in the job bucket.

        while loop to run jobs:
        1. worker get jobs from job bucket
        2. worker run jobs
        3. worker validate jobs
        worker quit when there is no job to run or timeout
        """
