import multiprocessing
import os
import pathlib
import subprocess

import pandas as pd
import yaml

from .template import rander_preset_config
from .utilities import CommandRunner


def _get_mem_gb():
    mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    mem_gb = mem_bytes / (1024**3)
    return mem_gb


class RobustSnakemakeRunner:
    """A wrapper around Snakemake that can recover from preemption."""

    def __init__(
        self,
        work_dir,
        snakefile=None,
        n_jobs=None,
        n_mem_gb=None,
        retry=2,
        keep_going=True,
        latency_wait=10,
        sleep_after_fail=3600,
        check=False,
    ):
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
        self.sleep_after_fail = sleep_after_fail
        self.check = check

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
            f"--resources mem_gb={int(self.n_mem_gb)} "
            f"--rerun-incomplete "
            f"{'--keep-going' if self.keep_going else ''} "
            f"--latency-wait {self.latency_wait} "
            f"--restart-times {self.retry}"
        )

        # here snakemake failures are not checked, result will be transfer to bucket anyway
        CommandRunner(
            cmd,
            log_prefix=self.work_dir / "snakemake",
            check=self.check,
            retry=1,
            sleep_after_fail=self.sleep_after_fail,
        ).run()

        self.cleanup()
        return

    def cleanup(self):
        """Cleanup Snakemake after run."""
        snakemake_log_dir = self.work_dir / ".snakemake"
        subprocess.run(f"rm -rf {snakemake_log_dir}", shell=True, check=True)
        return


def prepare_snakemake(
    job_dir,
    template_dir,
    groups,
    output_bucket,
    output_prefix,
    sky_template,
    total_jobs=None,
    total_mem_gb=None,
    default_cpu=1,
    default_mem_gb=1,
    sleep_after_fail=60,
    batches=None,
):
    """Prepare snakemake cloud config."""
    job_dir = pathlib.Path(job_dir).resolve().absolute()
    job_dir.mkdir(parents=True, exist_ok=True)
    template_dir = pathlib.Path(template_dir).resolve().absolute()

    if isinstance(groups, (str, pathlib.Path)):
        groups = pd.read_csv(groups, header=None, index_col=None).squeeze().tolist()

    if batches is None:
        batch_size = len(groups)
    else:
        batch_size = len(groups) // batches + 1

    group_table = pd.Series({group: i // batch_size for i, group in enumerate(groups)})
    for job_id, sub_df in group_table.groupby(group_table):
        work_dir = job_dir / str(job_id)
        work_dir.mkdir(parents=True, exist_ok=True)
        this_template_dir = work_dir / "template"
        this_template_dir.mkdir(parents=True, exist_ok=True)
        subprocess.run(f"cp -r {template_dir}/* {this_template_dir}", shell=True, check=True)

        groups_path = work_dir / "groups.txt"
        with open(groups_path, "w") as f:
            f.write("\n".join(sub_df.index))

        record = {
            "groups_path": str(groups_path),
            "templates_path": str(this_template_dir),
            "bucket": output_bucket,
            "prefix": f"{output_prefix}/{job_id}",
            "total_jobs": total_jobs,
            "total_mem_gb": total_mem_gb,
            "default_cpu": default_cpu,
            "default_mem_gb": default_mem_gb,
            "job_id": job_id,
            "sleep_after_fail": sleep_after_fail,
        }

        out_config = job_dir / f"{job_id}.config.yaml"
        uploaded_config = job_dir / f"{job_id}.config.yaml_uploaded"
        if uploaded_config.exists():
            continue
        rander_preset_config("snakemake", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    with open(sky_template) as f:
        sky_template = yaml.load(f, Loader=yaml.FullLoader)
        # remove the run section of template
        sky_template.pop("run", None)
        sky_template["setup"] += "\n pip install git+https://github.com/lhqing/gliderport.git"
    with open(out_sky, "w") as f:
        yaml.dump(sky_template, f, default_style="|")
    return
