import pathlib

import pandas as pd

from .template import rander_preset_config, rander_preset_sky


def prepare_merge_allc(
    allc_table,
    chrom_size_cloud_path,
    job_dir,
    output_bucket,
    output_prefix,
    instance="n2d-highcpu-48",
    region="us-west1",
):
    """Prepare merge allc cloud config."""
    job_dir = pathlib.Path(job_dir)
    job_dir.mkdir(exist_ok=True, parents=True)

    # prepare job config
    allc_table = pd.read_csv(allc_table, header=None, names=["allc_path", "group"])
    for group, sub_df in allc_table.groupby("group"):
        record = {
            "preset": "merge_allc",
            "bucket": output_bucket,
            "prefix": output_prefix,
            "chrom_size_cloud_path": chrom_size_cloud_path,
            "output_name": f"{group}.allc.tsv.gz",
            "allc_paths": sub_df["allc_path"].tolist(),
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
