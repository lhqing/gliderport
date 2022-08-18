"""
Worker and Job classes for the VM.

Format of job config:
- Input: bucket and prefix to get files from GCS
- Output: bucked and prefix to upload files to GCS
- Complete: flag to indicate job is complete
- Run: a list of commands to run on the VM to generate the output files
- DeleteInput: True or False, whether to delete the input files on source GCS after the job is done
- LocalPrefix: Prefix to put the input files on the local PD, if none, use the home directory
"""

import os
import shutil
import subprocess
import time
from pathlib import Path
from tempfile import TemporaryDirectory

import yaml

from .files import FileDownloader, FileUploader


class Job:
    """Job created from a config file."""

    def __init__(self, config_file):
        self.config_file = config_file
        self.config = self._read_config()
        self._delete_input_from_gcs_flag = self.config.get("delete_input", False)

        self._local_prefix = Path(self.config["local_prefix"]).absolute().resolve()
        self._local_prefix.mkdir(exist_ok=True, parents=True)
        print(f"Local prefix is {self._local_prefix}")

        self.file_downloader = FileDownloader(
            bucket=self.config["input"]["bucket"], prefix=self.config["input"]["prefix"], dest_path=self._local_prefix
        )

        self.file_uploader = FileUploader(
            bucket=self.config["output"]["bucket"],
            prefix=self.config["output"]["prefix"],
            file_paths=self._local_prefix,
            config_file=None,
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

    def _read_config(self):
        with open(self.config_file) as fp:
            config = yaml.load(fp, Loader=yaml.FullLoader)

        assert "local_prefix" in config, "local_prefix must be specified in config"
        assert "input" in config, "Input is not defined in config"
        assert "output" in config, "Output is not defined in config"
        assert "run" in config, "Worker is not defined in config"
        return config

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

    def __init__(self, job_bucket, job_prefix, max_idle_time=600):
        with TemporaryDirectory() as tmp_dir:
            self.local_config_dir = Path(tmp_dir)

            # init job configs
            super().__init__(job_bucket, job_prefix, self.local_config_dir, check_config=False)
            self.job_configs = []
            self.run(max_idle_time=max_idle_time)
            return

    def _update_job_configs(self):
        """Get jobs from job bucket."""
        self.transfer(redo=True)  # redo because we want to ignore download flag and get new configs
        self.job_configs = list(self.local_config_dir.glob("*.yaml"))

    def _delete_job_config(self, job_config):
        """For a successful job, delete the config from local and job bucket."""
        print(f"Job {job_config.name} done, delete config from job bucket.")

        # local
        job_config.unlink()

        # gcs
        gcs_path = f"gs://{self.bucket.name}/{self.prefix}/{job_config.name}"
        try:
            subprocess.run(f"gsutil rm {gcs_path}", shell=True, capture_output=True, encoding="utf-8", check=True)
        except subprocess.CalledProcessError as e:
            print(e.output)
            print(e.stderr)
            raise e
        return

    def run(self, max_idle_time=600):
        """Run the jobs in the job bucket."""
        idle = 0
        while True:
            self._update_job_configs()
            time.sleep(1)

            if len(self.job_configs) == 0:
                # no jobs, sleep for a while and retry
                idle += 60
                if idle > max_idle_time:
                    print("No job to run, worker quit.")
                    break
                else:
                    print(f"No job to run, sleep for {idle} seconds.")
                    time.sleep(idle)
            else:
                # reset idle time
                idle = 0

                # run jobs and delete configs when done
                for job_config in self.job_configs:
                    # run job
                    Job(job_config).run()
                    # delete job config from job bucket
                    self._delete_job_config(job_config)
        return
