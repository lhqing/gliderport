import pathlib

import pandas as pd

from .template import rander_preset_config, rander_preset_sky
from .utilities import _get_cpu_from_instance_name


def prepare_call_loop(
    job_dir,
    cool_table_csv,
    chrom_size_path,
    black_list_path,
    output_bucket,
    output_prefix,
    instance="n2d-standard-48",
    region="us-west1",
    disk_size=400,
):
    """Prepare call loop cloud config."""
    job_dir = pathlib.Path(job_dir).absolute().resolve()
    job_dir.mkdir(parents=True, exist_ok=True)

    cell_table = pd.read_csv(cool_table_csv, header=None, names=["cell_id", "cool_path", "group"])

    # prepare job config
    for group, sub_df in cell_table.groupby("group"):
        cell_table_path = f"{job_dir}/{group}.cell_table.csv"
        new_path_df = sub_df.copy()
        new_path_df["cool_path"] = new_path_df["cool_path"].apply(lambda p: f"~/sky_workdir/{pathlib.Path(p).name}")
        new_path_df.to_csv(cell_table_path, index=False, header=False)

        record = {
            "cell_table": cell_table_path,
            "output_dir": group,
            "output_bucket": output_bucket,
            "cool_paths": sub_df["cool_path"].tolist(),
            "output_prefix": output_prefix,
            "chrom_size_path": chrom_size_path,
            "chrom_size_file": chrom_size_path,
            "black_list_file": black_list_path,
            "cell_table_file_name": f"{group}.cell_table.csv",
            "chrom_size_file_name": pathlib.Path(chrom_size_path).name,
            "black_list_file_name": pathlib.Path(black_list_path).name,
            "n_cpu": _get_cpu_from_instance_name(instance),
        }

        out_config = job_dir / f"{group}.config.yaml"
        uploaded_config = job_dir / f"{group}.config.yaml_uploaded"
        if uploaded_config.exists():
            continue
        rander_preset_config("call_loop", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    record = {
        "instance": instance,
        "region": region,
        "disk_size": disk_size,
    }
    rander_preset_sky("call_loop", out_sky, **record)
    return
