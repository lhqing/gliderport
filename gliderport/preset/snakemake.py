import multiprocessing
import os
import pathlib

from .utilities import CommandRunner


def _get_mem_gb():
    mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    mem_gb = mem_bytes / (1024**3)
    return mem_gb


class RobustSnakemakeRunner:
    """A wrapper around Snakemake that can recover from preemption."""

    def __init__(self, work_dir, snakefile=None, n_jobs=None, n_mem_gb=None, retry=2, keep_going=True, latency_wait=60):
        if n_jobs is None:
            self.n_jobs = multiprocessing.cpu_count()
        else:
            self.n_jobs = n_jobs

        if n_mem_gb is None:
            self.n_mem_gb = _get_mem_gb()
        else:
            self.n_mem_gb = n_mem_gb

        self.work_dir = pathlib.Path(work_dir).resolve().absolute()

        if snakefile is None:
            self.snakefile = self.work_dir / "Snakefile"
        else:
            self.snakefile = pathlib.Path(snakefile).resolve().absolute()

        self.retry = retry
        self.keep_going = keep_going
        self.latency_wait = latency_wait

    def _unlock(self):
        """Unlock Snakemake."""
        lock_dir = self.work_dir / ".snakemake/locks"
        if len(list(pathlib.Path(lock_dir).glob("*"))) > 0:
            cmd = f"snakemake -d {self.work_dir} --snakefile {self.snakefile} --unlock"
            CommandRunner(cmd, log_prefix=None, check=True).run()

    def run(self):
        """Run Snakemake."""
        self._unlock()
        cmd = (
            f"snakemake "
            f"-d {self.work_dir} "
            f"--snakefile {self.snakefile} "
            f"--cores {self.n_jobs} "
            f"--resources mem_gb={self.n_mem_gb} "
            f"--rerun-incomplete "
            f"{'--keep-going' if self.keep_going else ''} "
            f"--latency-wait {self.latency_wait} "
            f"--restart-times {self.retry}"
        )
        CommandRunner(cmd, log_prefix=self.work_dir / "snakemake", check=True, retry=1).run()
        return


# def prepare_snakemake(
#     job_dir,
#     work_dirs,
#     output_bucket,
#     output_prefix,
#     sky_template,
#     instance="n2d-standard-48",
#     region="us-west1",
#     disk_size=256,
# ):
#     """Prepare snakemake cloud config."""
#     job_dir = pathlib.Path(job_dir).absolute().resolve()
#     job_dir.mkdir(parents=True, exist_ok=True)
#
#     if isinstance(work_dirs, (str, pathlib.Path)):
#         work_dirs = [work_dirs]
#     work_dirs = [pathlib.Path(wd).absolute().resolve() for wd in work_dirs]
#
#     # prepare job config
#     #for work_dir in work_dirs:
#     #    job_dir / "config.yaml"
#
#     # prepare sky template
#     out_sky = job_dir / "SKY_TEMPLATE.yaml"
#     record = {
#         "instance": instance,
#         "region": region,
#         "disk_size": disk_size,
#         "output_bucket": output_bucket,
#     }
#     rander_preset_sky("snakemake", out_sky, **record)
#     return
