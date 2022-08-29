import pathlib
import re

import pandas as pd

from .template import rander_preset_config, rander_preset_sky

R1_PATTERN = re.compile("((R1)|(r1)).((fastq)|(fq))(.gz){0,1}")
R2_PATTERN = re.compile("((R2)|(r2)).((fastq)|(fq))(.gz){0,1}")


def _make_fastq_table_from_library_dirs(library_dirs):
    if isinstance(library_dirs, (str, pathlib.Path)):
        library_dirs = [library_dirs]

    _fastq_dirs = []
    for library_dir in library_dirs:
        library_dir = pathlib.Path(library_dir).absolute().resolve()
        if not library_dir.exists():
            raise ValueError(f"Library dir {library_dir} does not exist.")
        fastq_dirs = list(library_dir.glob("*/fastq"))
        _fastq_dirs += fastq_dirs
    return _make_fastq_table_from_fastq_dirs(_fastq_dirs)


def _make_fastq_table_from_fastq_dirs(fastq_dirs):
    if isinstance(fastq_dirs, (str, pathlib.Path)):
        fastq_dirs = [fastq_dirs]

    fastq_collection = {"R1": {}, "R2": {}}
    for fastq_dir in fastq_dirs:
        fastq_dir = pathlib.Path(fastq_dir).absolute().resolve()
        if not fastq_dir.exists():
            raise ValueError(f"Fastq dir {fastq_dir} does not exist.")
        fastq_paths = list(fastq_dir.glob("*.gz"))
        for fastq_path in fastq_paths:
            cell_id_read_type = fastq_path.name.split(".")[0]
            *cell_id, read_type = cell_id_read_type.split("-")
            cell_id = "-".join(cell_id)
            fastq_collection[read_type][cell_id] = str(fastq_path)
    fastq_table = pd.DataFrame.from_dict(fastq_collection).dropna()
    fastq_table["group"] = fastq_table["R1"].apply(lambda x: pathlib.Path(x).parent.parent.name)
    return fastq_table


def prepare_mapping(
    snakefile_cloud_path,
    job_dir,
    output_bucket,
    output_prefix,
    image_id,
    disk_size=256,
    library_dirs=None,
    fastq_dirs=None,
    fastq_table=None,
    auto_group_size=64,
    instance="n2d-highcpu-64",
    region="us-west1",
    mapping_yaml_path="../mapping.yaml",
):
    """Prepare mapping cloud config."""
    job_dir = pathlib.Path(job_dir)
    job_dir.mkdir(exist_ok=True, parents=True)

    # prepare fastq table
    if fastq_table is None:
        if library_dirs is not None:
            fastq_table = _make_fastq_table_from_library_dirs(library_dirs)
        elif fastq_dirs is not None:
            fastq_table = _make_fastq_table_from_fastq_dirs(fastq_dirs)
        else:
            raise ValueError("One of fastq_table or fastq_dirs or library_dirs must be provided.")
    else:
        fastq_table = pd.read_csv(fastq_table, header=None)
        if fastq_table.shape[1] == 3:
            fastq_table.columns = ["R1", "R2", "group"]
        elif fastq_table.shape[1] == 2:
            fastq_table.columns = ["R1", "R2"]
            # assign group if group column is not provided
            fastq_table["group"] = [i // auto_group_size for i in range(fastq_table.shape[0])]
            fastq_table["group"] = fastq_table["group"].apply(lambda i: f"mapping_{i:06d}")
        else:
            raise ValueError("fastq_table must have ['R1', 'R2'] or ['R1', 'R2', 'group'] columns.")

    fastq_table.to_csv(job_dir / "fastq_table.csv", index=False)

    output_prefix = str(output_prefix).rstrip("/")

    # prepare job config
    for group, sub_df in fastq_table.groupby("group"):
        r1_paths = sub_df["R1"].tolist()
        r2_paths = sub_df["R2"].tolist()
        record = {
            "preset": "mapping",
            "r1_paths": r1_paths,
            "r2_paths": r2_paths,
            "bucket": output_bucket,
            "prefix": f"{output_prefix}/{group}",
            "snakefile_cloud_path": snakefile_cloud_path,
            "mapping_yaml_path": mapping_yaml_path,
        }

        out_config = job_dir / f"{group}.config.yaml"
        uploaded_config = job_dir / f"{group}.config.yaml_uploaded"
        if uploaded_config.exists():
            continue
        rander_preset_config("mapping", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    rander_preset_sky("mapping", out_sky, instance=instance, region=region, image_id=image_id, disk_size=disk_size)
    return
