"""Worker and Job classes for the VM."""

import os
import shutil
import time
from pathlib import Path
from tempfile import NamedTemporaryFile, TemporaryDirectory

import gcsfs

from .files import FileDownloader, FileUploader
from .log import init_logger
from .utils import CommandRunner, read_config

logger = init_logger(__name__)


class Job:
    """Job created from a config file."""

    def __init__(self, config_file):
        self.config_file = config_file
        self.config, _ = read_config(config_file, input_mode="gcs")
        self._delete_input_from_gcs_flag = self.config.get("delete_input", False)

        self._local_prefix = Path(f"{os.environ['HOME']}/sky_workdir/")
        self._local_prefix.mkdir(parents=True, exist_ok=True)

        logger.info(f"Local prefix on VM is {self._local_prefix}")
        self.file_downloader = FileDownloader(
            bucket=self.config["input"]["bucket"], prefix=self.config["input"]["prefix"], dest_path=self._local_prefix
        )

        if "output" in self.config:
            self.file_uploader = FileUploader(
                bucket=self.config["output"]["bucket"],
                prefix=self.config["output"]["prefix"],
                file_paths=self._local_prefix,
            )
        else:
            self.file_uploader = None

        if "run" in self.config:
            self.runner = "bash"
            self.run_commands = self.config["run"]
        elif "python" in self.config:
            self.runner = "python"
            self.run_commands = self.config["python"]
        else:
            raise ValueError(f"No run or python command found in config file {self.config_file}")

    def _run(self, log_prefix, retry=2, check=False):
        commands = self.run_commands

        if isinstance(commands, str):
            commands = [commands]

        # change working directory to local prefix
        previous_cwd = os.getcwd()
        os.chdir(self._local_prefix)
        with NamedTemporaryFile(mode="w", delete=False) as f:
            for command in commands:
                f.write(f"{command}\n")
            f.flush()

            # run commands
            cmd = f"{self.runner} {f.name}"
            success_flag = CommandRunner(retry=retry, command=cmd, log_prefix=log_prefix, check=check).run()
        # change back to previous working directory
        os.chdir(previous_cwd)
        return success_flag

    def _clear_local_files(self):
        # clean up local files
        for files in os.listdir(self._local_prefix):
            path = os.path.join(self._local_prefix, files)
            try:
                shutil.rmtree(path)
            except OSError:
                os.remove(path)

    def run(self, log_prefix=None, retry=2, check=False):
        """Run the job, return True if successful."""
        logger.info(f"Running job with config file {self.config_file}")

        # make sure local prefix is empty
        self._clear_local_files()

        self.file_downloader.transfer()
        # remove download success flag after making sure it exists
        assert self.file_downloader.download_success_path.exists(), "Download success flag does not exist"
        self.file_downloader.download_success_path.unlink()

        success_flag = self._run(log_prefix=log_prefix, retry=retry, check=check)

        if success_flag:
            if self.file_uploader is not None:
                self.file_uploader.transfer()

        # clean up local files for the next job
        self._clear_local_files()

        # delete input from GCS if flag is set and the job was successful
        if success_flag and self._delete_input_from_gcs_flag:
            self.file_downloader.delete_source()
        return success_flag


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

    def _mark_job_config(self, job_config, suffix):
        """For a successful job, rename the config at local and job bucket."""
        logger.info(f"Job {job_config.name} done, delete config from job bucket.")

        # local
        job_config.rename(job_config.with_name(job_config.name + suffix))

        # gcs
        gcs_path = f"{self.bucket.name}/{self.prefix}/{job_config.name}"
        self._fs.rename(gcs_path, gcs_path + suffix)
        return

    def run(self, max_idle_time, retry=2):
        """Run the jobs in the job bucket."""
        total_idle_time = 0
        while True:
            self._update_job_configs()
            time.sleep(2)

            if len(self.job_configs) == 0:
                # no jobs, sleep for a while and retry
                total_idle_time += 60
                if total_idle_time > max_idle_time:
                    logger.info("No job to run, worker quit.")
                    break
                else:
                    logger.info("No job to run, sleep for 60 seconds.")
                    time.sleep(60)
            else:
                # reset idle time
                total_idle_time = 0

                # run jobs and delete configs when done
                for job_config in self.job_configs:
                    # run job
                    log_prefix = f"gs://{self.bucket.name}/{self.prefix}/{job_config.name}"
                    success_flag = Job(job_config).run(log_prefix=log_prefix, retry=retry, check=False)

                    # rename job config in job bucket, the same config will not be run again
                    if success_flag:
                        self._mark_job_config(job_config, suffix="_success")
                    else:
                        self._mark_job_config(job_config, suffix="_fail")
        return
