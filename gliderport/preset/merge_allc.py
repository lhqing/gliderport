import pathlib

import pandas as pd
from sky.sky_logging import init_logger

from .template import rander_preset_config, rander_preset_sky
from .utilities import _get_cpu_from_instance_name

logger = init_logger(__name__)


def _add_tbi(allc_table):
    tib_table = allc_table.copy()
    tib_table["allc_path"] = tib_table["allc_path"] + ".tbi"
    return pd.concat([allc_table, tib_table])


def prepare_merge_allc(
    allc_table_csv,
    chrom_size_cloud_path,
    job_dir,
    output_bucket,
    output_prefix,
    instance="n2d-standard-48",
    region="us-west1",
    merge_allc_cpu=None,
):
    """Prepare merge allc cloud config."""
    job_dir = pathlib.Path(job_dir)
    job_dir.mkdir(exist_ok=True, parents=True)

    # prepare job config
    allc_table = pd.read_csv(allc_table_csv, header=None, names=["allc_path", "group"])
    # add tbi paths for each allc file
    allc_table = _add_tbi(allc_table)
    output_prefix = str(output_prefix).rstrip("/")

    for group, sub_df in allc_table.groupby("group"):
        record = {
            "preset": "merge_allc",
            "bucket": output_bucket,
            "prefix": f"{output_prefix}/{group}",
            "chrom_size_cloud_path": chrom_size_cloud_path,
            "output_name": f"{group}.allc.tsv.gz",
            "allc_paths": sub_df["allc_path"].tolist(),
            "cpu": _get_cpu_from_instance_name(instance) if merge_allc_cpu is None else merge_allc_cpu,
        }

        out_config = job_dir / f"{group}.config.yaml"
        uploaded_config = job_dir / f"{group}.config.yaml_uploaded"
        if uploaded_config.exists():
            continue

        rander_preset_config("merge_allc", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    rander_preset_sky("merge_allc", out_sky, instance=instance, region=region)
    return
