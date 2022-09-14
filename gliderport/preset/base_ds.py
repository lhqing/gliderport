import pathlib

import pandas as pd

from .template import rander_preset_config, rander_preset_sky
from .utilities import _get_cpu_from_instance_name


def prepare_base_ds(
    job_dir,
    allc_table_csv,
    chrom_size_path,
    remote_genome_path,
    input_bucket,
    output_bucket,
    output_prefix,
    instance="n2d-standard-48",
    region="us-west1",
    pos_chunk=1000000,
    sample_chunk=200,
):
    """Prepare base ds cloud config."""
    job_dir = pathlib.Path(job_dir).absolute().resolve()
    job_dir.mkdir(parents=True, exist_ok=True)

    if not str(remote_genome_path).startswith("gs://"):
        raise ValueError("remote_genome_path must be a gs:// path")

    allc_table = pd.read_csv(allc_table_csv, header=None, names=["sample_id", "allc_path"])
    chrom_sizes = pd.read_csv(chrom_size_path, header=None, names=["chrom", "size"], sep="\t")

    # add sample group
    allc_table["group"] = [f"group{i // sample_chunk}" for i in range(allc_table.shape[0])]

    # save each chromosome size to a separate file
    chrom_size_path_dict = {}
    for chrom, size in chrom_sizes.values:
        chrom_size_path = f"{job_dir}/{chrom}.size"
        with open(chrom_size_path, "w") as f:
            f.write(f"{chrom}\t{size}\n")
        chrom_size_path_dict[chrom] = chrom_size_path

    # prepare job config
    output_prefix = str(output_prefix).rstrip("/")
    for chrom, chrom_path in chrom_size_path_dict.items():
        for group, sub_df in allc_table.groupby("group"):
            job_name = f"{group}_{chrom}"
            allc_table_path = f"{job_dir}/{job_name}.allc_table.csv"
            sub_df[["sample_id", "allc_path"]].to_csv(allc_table_path, index=False, header=False)
            base_ds_path = f"/output/{output_prefix}/{group}.baseds"
            record = {
                "allc_table": allc_table_path,
                "chrom_size_file": chrom_path,
                "base_ds_path": base_ds_path,
                "chrom_size_file_name": pathlib.Path(chrom_path).name,
                "allc_table_name": pathlib.Path(allc_table_path).name,
                "pos_chunk": pos_chunk,
                "sample_chunk": sample_chunk,
                "job_name": job_name,
                "n_cpu": _get_cpu_from_instance_name(instance),
            }

            out_config = job_dir / f"{job_name}.config.yaml"
            uploaded_config = job_dir / f"{job_name}.config.yaml_uploaded"
            if uploaded_config.exists():
                continue
            rander_preset_config("generate_baseds", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    record = {
        "instance": instance,
        "region": region,
        "genome_path": remote_genome_path,
        "input_bucket": input_bucket,
        "output_bucket": output_bucket,
    }
    rander_preset_sky("generate_baseds", out_sky, **record)
    return
