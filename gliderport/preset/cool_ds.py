import pathlib

import pandas as pd

from .template import rander_preset_config, rander_preset_sky
from .utilities import _get_cpu_from_instance_name


def prepare_cool_ds(
    job_dir,
    cool_table_csv,
    chrom_size_path,
    value_types,
    input_bucket,
    output_bucket,
    output_prefix,
    trans_matrix=False,
    sample_chunk=50,
    instance="n2d-standard-48",
    region="us-west1",
    image_id=None,
    disk_size=512,
):
    """Prepare cool ds cloud config."""
    job_dir = pathlib.Path(job_dir).absolute().resolve()
    job_dir.mkdir(parents=True, exist_ok=True)

    cool_table = pd.read_csv(cool_table_csv, header=None, names=["sample", "value_type", "path", "cool_type"])
    # add sample group
    sample_to_group_id = {
        sample: f"group{i // sample_chunk}" for i, sample in enumerate(cool_table["sample"].sort_values().unique())
    }
    cool_table["group"] = cool_table["sample"].map(sample_to_group_id)

    # prepare job config
    output_prefix = str(output_prefix).rstrip("/")
    for group, sub_df in cool_table.groupby("group"):
        job_name = group
        cool_table_path = f"{job_dir}/{job_name}.cool_table.csv"

        sub_df["path"] = f"~/{job_name}/" + sub_df["path"].apply(lambda x: pathlib.Path(x).name)
        sub_df[["sample", "value_type", "path", "cool_type"]].to_csv(cool_table_path, index=False, header=False)
        cool_ds_path = f"/output/{output_prefix}/{group}.coolds"
        record = {
            "cool_paths": sub_df["path"].tolist(),
            "cool_table": cool_table_path,
            "chrom_size_file": chrom_size_path,
            "cool_ds_path": cool_ds_path,
            "job_name": job_name,
            "cool_table_name": pathlib.Path(cool_table_path).name,
            "value_types": value_types,
            "chrom_size_file_name": pathlib.Path(chrom_size_path).name,
            "trans_matrix": trans_matrix,
            "sample_chunk": sample_chunk,
            "n_cpu": _get_cpu_from_instance_name(instance),
        }

        out_config = job_dir / f"{job_name}.config.yaml"
        uploaded_config = job_dir / f"{job_name}.config.yaml_uploaded"
        if uploaded_config.exists():
            continue
        rander_preset_config("generate_coolds", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    record = {
        "instance": instance,
        "region": region,
        "image_id": image_id,
        "disk_size": disk_size,
        "input_bucket": input_bucket,
        "output_bucket": output_bucket,
    }
    rander_preset_sky("generate_coolds", out_sky, **record)
    return
