import enum
import random
import subprocess
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from tempfile import NamedTemporaryFile

import gcsfs
import pandas as pd
import yaml
from sky import core as sky_core
from sky import exceptions as sky_exceptions

from .config import read_config
from .files import FileUploader

WORKER_REFRESH_CLOCK_INIT = 16


def _check_spot_controller_up():
    print("Checking sky spot controller status")
    sky_status = sky_core.status(refresh=True, all=True)
    controller_up = False
    for cluster in sky_status:
        if "sky-spot-controller" in cluster["name"]:
            if cluster["status"].name == "UP":
                controller_up = True
                break
    return controller_up


def _check_spot_status():
    try:
        print("Checking sky spot jobs status")
        sky_spot_status = sky_core.spot_status(refresh=True)
    except sky_exceptions.ClusterNotUpError:
        sky_spot_status = []
    return sky_spot_status


def _upload(**kwargs):
    job_id = kwargs.pop("job_id")
    print(f"Uploading job {job_id}")
    FileUploader(**kwargs).transfer()
    return


def _null_upload(job_id):
    print(f"Uploading job {job_id}")
    return


def _get_hash():
    _hash = random.getrandbits(32)
    _hash = f"{_hash:x}"
    return _hash


class _JobListener:
    """
    Job Listener detects new jobs and upload them to GCS if needed.

    Listen to a directory for new job files, start data uploader and return job_id when file is ready on GCS.
    """

    def __init__(self, local_job_dir, bucket, n_jobs=1):
        self.local_job_dir = local_job_dir
        self.bucket = bucket
        self.n_jobs = n_jobs

    def get_job_configs(self) -> pd.Series:
        """Get all jobs in local_job_dir."""
        configs = {".".join(p.name.split(".")[:-2]): p for p in self.local_job_dir.glob("*.config.yaml")}
        job_configs = pd.Series(configs).sort_index()
        return job_configs

    def _parse_job_config(self, job_id, config_path):
        config, input_opt = read_config(config_path)

        if input_opt == "local":
            file_paths = config["input"]["local"]
            # change input mode to gcs, and return new config file
            config["input"]["bucket"] = self.bucket
            prefix = f"{job_id}_input"
            config["input"]["prefix"] = prefix
            del config["input"]["local"]

            with NamedTemporaryFile(suffix=".config.yaml", delete=False, mode="w") as temp_config_file:
                yaml.dump(config, temp_config_file, default_style="|")
            return input_opt, file_paths, prefix, temp_config_file.name

        elif input_opt == "gcs":
            return input_opt

        else:
            raise ValueError(f"Unknown input option {input_opt}")

    def upload_and_get_prefix(self):
        """Upload all jobs in current local_job_dir."""
        job_configs = self.get_job_configs()

        with ProcessPoolExecutor(self.n_jobs) as executor:
            futures = {}
            for job_id, local_config_path in job_configs.items():
                input_opt, *values = self._parse_job_config(job_id, local_config_path)
                if input_opt == "local":
                    file_paths, prefix, temp_config_file = values
                    future = executor.submit(
                        _upload,
                        job_id=job_id,
                        bucket=self.bucket,
                        prefix=prefix,
                        file_paths=file_paths,
                        file_list_path=None,
                    )
                    futures[future] = job_id, temp_config_file, local_config_path
                elif input_opt == "gcs":
                    future = executor.submit(_null_upload, job_id=job_id)
                    futures[future] = job_id, local_config_path, local_config_path
                else:
                    raise ValueError(f"Unknown input option {input_opt}")

            for future in as_completed(futures):
                job_id, config_path, local_config_path = futures[future]
                future.result()
                yield job_id, config_path, local_config_path


class NullStatus(enum.Enum):
    """Spot job status, designed to be in serverless style."""

    # mimic the sky SpotStatus class
    NOT_SUBMIT = "NOT_SUBMIT"

    @staticmethod
    def is_terminal():
        """Return True if the status is terminal."""
        return True

    @staticmethod
    def is_failed():
        """Return True if the status is failed."""
        return False


class _SpotWorker:
    """Spot worker class."""

    def __init__(
        self, worker_id, template_dict, bucket, job_config_dir, worker_hash, launch_timeout=600, max_idle_time=600
    ):
        """
        Initialize a spot worker.

        Parameters
        ----------
        worker_id :
            worker id integer, one worker only has one active spot job
        template_dict :
            template dictionary for sky spot launch vm
        bucket :
            bucket name for the spot job to monitor job configs
        job_config_dir :
            directory prefix for the spot job to monitor job configs
        worker_hash :
            worker hash for the spot job name
        launch_timeout :
            timeout for spot job launch process
        max_idle_time :
            max idle time for spot job vm worker
        """
        self.worker_id = worker_id

        self.bucket = bucket
        self.job_config_dir = job_config_dir
        self.prefix = f"{job_config_dir}/worker_{self.worker_id}"
        self.max_idle_time = max_idle_time
        self.worker_hash = worker_hash

        self._status = {
            "job_id": None,
            "job_name": self.job_name,
            "resources": None,
            "submitted_at": None,
            "status": NullStatus.NOT_SUBMIT,
            "run_timestamp": None,
            "start_at": None,
            "end_at": None,
            "last_recovered_at": None,
            "recovery_count": None,
            "job_duration": None,
        }
        self._jobs = {}

        self._template = template_dict
        self._launch_process = None
        self._launch_failed_count = 0
        self._launch_time = None
        self._launch_timeout = launch_timeout

    def update_status(self, status):
        if not isinstance(status, dict):
            return

        if status["job_name"] != self._status["job_name"]:
            raise ValueError(f"Job name {status['job_name']} does not match {self._status['job_name']}")

        # put status into self._jobs
        self._jobs[status["job_id"]] = status

        # if a worker is submitted multiple times, there will be multiple status with this job_name
        # only update the newest status
        # get the status with latest submitted_at time
        latest_job_status = sorted(self._jobs.values(), key=lambda i: i["submitted_at"])[-1]
        self._status.update(latest_job_status)
        return

    def launch(self):
        """Launch a job on the spot cluster."""
        worker_config = self._template.copy()
        run_cmd = (
            "glider vm-worker "
            f"--bucket {self.bucket} "
            f"--prefix {self.prefix} "
            f"--max_idle_time {self.max_idle_time}"
        )
        worker_config["run"] = run_cmd

        with NamedTemporaryFile(suffix=".sky-worker.yaml", mode="w") as temp_config_file:
            yaml.dump(worker_config, temp_config_file, default_style="|")

            cmd = f"sky spot launch -y -d -n {self.job_name} {temp_config_file.name}"

            print(f"Launching worker {self.worker_id}\n{cmd}")
            self._launch_process = subprocess.Popen(
                cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8"
            )
            self._launch_time = time.time()
        return

    def check_launch(self):
        """Check worker status before launch process."""
        if self._launch_process is not None:
            try:
                self._launch_process.wait(timeout=0.1)
                # the previous launch process is finished
                # check status and ready to launch next job
                if self._launch_process.returncode != 0:
                    self._launch_failed_count += 1
                    print(f"Launch worker {self.worker_id} failed {self._launch_failed_count} times")
                    print(self._launch_process.stderr.read())
                else:
                    self._launch_failed_count = 0
                self._launch_process = None

            except subprocess.TimeoutExpired:
                cur_time = time.time()
                if cur_time - self._launch_time > self._launch_timeout:
                    print(f"Launch worker {self.worker_id} timeout")
                    self._launch_process.kill()
                    stdout, stderr = self._launch_process.communicate()
                    print(stdout)
                    print(stderr)
                    self._launch_failed_count += 1
                    self._launch_process = None

                else:
                    # still launching
                    return
            if self._launch_failed_count >= 3:
                raise RuntimeError(f"Launch worker {self.worker_id} failed 3 times")

        if not self.is_terminal:
            print(f"Worker {self.worker_id} is still working with status {self.status}")
            return
        else:
            self.launch()
            return

    @property
    def job_name(self):
        return f"worker-{self.worker_hash}-{self.worker_id}"

    @property
    def status(self):
        return self._status["status"]

    @property
    def is_terminal(self):
        return self.status.is_terminal()

    @property
    def is_failed(self):
        return self.status.is_failed()


class WorkerManager:
    """Manage sky spot-VM workers."""

    def __init__(
        self,
        job_bucket,
        sky_template_path,
        fs,
        worker_hash,
        n_workers=16,
        launch_timeout=600,
        max_idle_time=600,
        spot=True,
    ):
        """
        Initialize a worker manager.

        Parameters
        ----------
        job_bucket :
            bucket name for the spot jobs to monitor job configs
        sky_template_path :
            path to the sky template file
        fs :
            filesystem object
        worker_hash :
            worker hash for the spot job name
        n_workers :
            number of workers to launch
        launch_timeout :
            timeout for spot job launch process
        max_idle_time :
            max idle time for spot job vm worker
        spot :
            whether to use spot instance
        """
        self.bucket = job_bucket
        self.job_config_dir = "job_config"
        self.n_workers = n_workers
        self.alive_workers = set()
        self._fs = fs
        self._worker_hash = worker_hash

        with open(sky_template_path) as f:
            self._sky_template_config = yaml.full_load(f)

        self._worker_max_idle_time = max_idle_time
        self._worker_launch_timeout = launch_timeout
        self._workers = {}
        self._init_workers(spot)
        self._worker_jobs = {}
        self._update_remote_worker_jobs()

    def _init_workers(self, spot):
        if spot:
            workers = {
                i: _SpotWorker(
                    worker_id=i,
                    template_dict=self._sky_template_config,
                    bucket=self.bucket,
                    job_config_dir=self.job_config_dir,
                    max_idle_time=self._worker_max_idle_time,
                    worker_hash=self._worker_hash,
                    launch_timeout=self._worker_launch_timeout,
                )
                for i in range(self.n_workers)
            }
            self._workers.update(workers)
        else:
            raise NotImplementedError

    def _update_remote_worker_jobs(self):
        """Update the status of all remote workers' job config dir."""
        _cur_worker_jobs = {i: set() for i in range(self.n_workers)}
        for job_config in self._fs.glob(f"{self.bucket}/{self.job_config_dir}/**/*.config.yaml"):
            *_, worker, job_config_file_name = job_config.split("/")
            worker_id = int(worker.split("_")[-1])
            job_id = ".".join(job_config_file_name.split(".")[:-2])
            _cur_worker_jobs[worker_id].add(job_id)
        self._worker_jobs = _cur_worker_jobs
        print(_cur_worker_jobs)
        return

    def _get_most_available_worker(self):
        """Get the worker id with the least jobs."""
        return sorted(self._worker_jobs.items(), key=lambda i: len(i[1]))[0][0]

    def deposit_job(self, job_id, config_path):
        """Deposit job to worker."""
        # check if job is already on a worker
        for worker_id, jobs in self._worker_jobs.items():
            if job_id in jobs:
                print(f"Job {job_id} is already on worker {worker_id}")
                return 0

        worker_id = self._get_most_available_worker()
        print(f"Depositing job {job_id} to worker {worker_id}")

        gcs_path = f"{self.bucket}/{self.job_config_dir}/worker_{worker_id}/{job_id}.config.yaml"
        # transfer job to remote
        print(config_path)
        print(gcs_path)
        self._fs.put_file(config_path, gcs_path)
        # update worker jobs
        self._worker_jobs[worker_id].add(job_id)
        return 1

    def _update_spot_worker_status(self):
        spot_jobs = _check_spot_status()
        for job in spot_jobs:
            job_name = job["job_name"]
            if not job_name.startswith(f"worker-{self._worker_hash}-"):
                # not a gp-worker job under this manager
                continue

            worker_id = int(job["job_name"].split("-")[-1])
            self._workers[worker_id].update_status(job)

    def launch_workers(self):
        """Update worker job counts."""
        # update works for each worker from remote
        self._update_remote_worker_jobs()

        # update worker status
        self._update_spot_worker_status()

        # if worker has jobs to do, launch it
        for worker_id, jobs in self._worker_jobs.items():
            n_jobs = len(jobs)
            if n_jobs == 0:
                continue
            worker = self._workers[worker_id]
            worker.check_launch()
        return


class GliderPort:
    """
    Sky manager run on-prime.

    Determine how to arrange job listener and workers,
    how to distribute jobs to workers,
    how to control the speed,
    and how to decide when to stop.
    """

    def __init__(
        self, local_job_dir, n_uploader=1, n_worker=16, max_idle_time=600, launch_timeout=600, use_hash=None, spot=True
    ):
        """
        Initialize a glider port.

        Parameters
        ----------
        local_job_dir :
            local job directory for the glider port to monitor
        n_uploader :
            number of uploader to launch
        n_worker :
            number of workers to launch
        max_idle_time :
            max idle time for spot job vm worker
        launch_timeout :
            timeout for spot job launch process
        use_hash :
            worker hash for the spot job name
        spot :
            whether to use spot instance
        """
        self.local_job_dir = Path(local_job_dir).absolute().resolve()
        if use_hash is None:
            self.gliderport_hash = _get_hash()
        else:
            print("Using user provided hash:", use_hash)
            self.gliderport_hash = use_hash

        print(f"Initializing GliderPort, ID is {self.gliderport_hash}")
        self.bucket_name = f"gliderport_temp_{self.gliderport_hash}"
        self._fs = gcsfs.GCSFileSystem()
        if not self._fs.exists(self.bucket_name):
            self._fs.mkdir(self.bucket_name)

        self.job_listener = _JobListener(local_job_dir=self.local_job_dir, bucket=self.bucket_name, n_jobs=n_uploader)
        self.job_config_prefix = "job_config"

        self._worker_refresh_clock = WORKER_REFRESH_CLOCK_INIT
        _sky_template = self.local_job_dir / "SKY_TEMPLATE.yaml"
        self.worker_manager = WorkerManager(
            job_bucket=self.bucket_name,
            n_workers=n_worker,
            sky_template_path=_sky_template,
            spot=spot,
            fs=self._fs,
            worker_hash=self.gliderport_hash,
            max_idle_time=max_idle_time,
            launch_timeout=launch_timeout,
        )
        return

    def _update_worker(self):
        """Update worker status."""
        self._worker_refresh_clock = WORKER_REFRESH_CLOCK_INIT
        self.worker_manager.launch_workers()
        return

    def run(self, max_idle_hours=100):
        """
        Run GliderPort on-prime.

        Parameters
        ----------
        max_idle_hours :
            max idle hours for glider port to wait
        """
        max_idle_time = max_idle_hours * 3600
        idle_time = 0
        while True:
            jobs_deposited_in_this_loop = 0
            for job_id, config_path, local_config_path in self.job_listener.upload_and_get_prefix():
                flag = self.worker_manager.deposit_job(job_id, config_path)
                jobs_deposited_in_this_loop += flag

                print(f"{local_config_path} uploaded")
                # change local_config_path to uploaded config_path
                new_path = local_config_path.parent / f"{local_config_path.name}_uploaded"
                local_config_path.rename(new_path)

                self._worker_refresh_clock -= 1
                if self._worker_refresh_clock == 0:
                    self._update_worker()

            if jobs_deposited_in_this_loop == 0:
                if idle_time % 1800 == 0:
                    self._update_worker()
                    print(f"No jobs deposited, sleeping... ({idle_time}/{max_idle_time})")
                idle_time += 60
                if idle_time > max_idle_time:
                    break
                time.sleep(60)
            else:
                self._update_worker()
                # Reset idle time if jobs are deposited.
                idle_time = 0
        return

    def _delete_bucket(self):
        """Delete bucket."""
        self._fs.rm(self.bucket_name)
        return
