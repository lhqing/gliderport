"""Worker and Job classes for the VM."""

import os
import shutil
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import gcsfs

from .config import read_config
from .files import FileDownloader, FileUploader


class Job:
    """Job created from a config file."""

    def __init__(self, config_file):
        self.config_file = config_file
        self.config, _ = read_config(config_file, input_mode="gcs")
        self._delete_input_from_gcs_flag = self.config.get("delete_input", False)

        self._local_prefix = Path(self.config["vm_local_prefix"])
        subprocess.run(f"mkdir -p {self._local_prefix}", shell=True, check=True)

        print(f"Local prefix on VM is {self._local_prefix}")

        self.file_downloader = FileDownloader(
            bucket=self.config["input"]["bucket"], prefix=self.config["input"]["prefix"], dest_path=self._local_prefix
        )

        self.file_uploader = FileUploader(
            bucket=self.config["output"]["bucket"],
            prefix=self.config["output"]["prefix"],
            file_paths=self._local_prefix,
        )
        self.run_commands = self.config["run"]

    def _run(self):
        commands = self.run_commands

        if isinstance(commands, str):
            commands = [commands]

        # change working directory to local prefix
        previous_cwd = os.getcwd()
        os.chdir(self._local_prefix)
        for command in commands:
            try:
                print(f"Running command: {command}")
                subprocess.run(command, shell=True, capture_output=True, check=True, encoding="utf-8")
            except subprocess.CalledProcessError as e:
                print(e.output)
                print(e.stderr)
                raise e
        # change back to previous working directory
        os.chdir(previous_cwd)
        return

    def run(self):
        """Run the job."""
        print(f"Running job with config file {self.config_file}")
        self.file_downloader.transfer()
        # remove download success flag after making sure it exists
        assert self.file_downloader.download_success_path.exists(), "Download success flag does not exist"
        self.file_downloader.download_success_path.unlink()

        self._run()

        self.file_uploader.transfer()

        # delete input from PD
        shutil.rmtree(self._local_prefix)

        # delete input from GCS if flag is set
        if self._delete_input_from_gcs_flag:
            self.file_downloader.delete_source()
        return


class Worker(FileDownloader):
    """Worker run inside VM, identify jobs to run by itself, stop when there is no job to run for a while."""

    def __init__(self, job_bucket, job_prefix, max_idle_time=1200):
        with TemporaryDirectory() as tmp_dir:
            self.local_config_dir = Path(tmp_dir)
            self._fs = gcsfs.GCSFileSystem()

            # init job configs
            super().__init__(job_bucket, job_prefix, self.local_config_dir)
            self.job_configs = []
            self.run(max_idle_time=max_idle_time)
            return

    def _update_job_configs(self):
        """Get jobs from job bucket."""
        self.transfer(redo=True)  # redo because we want to ignore download flag and get new configs
        self.job_configs = list(self.local_config_dir.glob("*.yaml"))

    def _mark_job_config_finish(self, job_config):
        """For a successful job, rename the config at local and job bucket."""
        print(f"Job {job_config.name} done, delete config from job bucket.")

        # local
        job_config.rename(job_config.with_name(job_config.name + "_finish"))

        # gcs
        gcs_path = f"{self.bucket.name}/{self.prefix}/{job_config.name}"
        self._fs.rename(gcs_path, gcs_path + "_finish")
        return

    def run(self, max_idle_time):
        """Run the jobs in the job bucket."""
        total_idle_time = 0
        while True:
            self._update_job_configs()
            time.sleep(2)

            if len(self.job_configs) == 0:
                # no jobs, sleep for a while and retry
                total_idle_time += 60
                if total_idle_time > max_idle_time:
                    print("No job to run, worker quit.")
                    break
                else:
                    print("No job to run, sleep for 60 seconds.")
                    time.sleep(60)
            else:
                # reset idle time
                total_idle_time = 0

                # run jobs and delete configs when done
                for job_config in self.job_configs:
                    # run job
                    Job(job_config).run()
                    # delete job config from job bucket
                    self._mark_job_config_finish(job_config)
        return
