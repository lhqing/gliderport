import pathlib

import pandas as pd
from sky.sky_logging import init_logger

from .template import rander_preset_config, rander_preset_sky
from .utilities import _get_cpu_from_instance_name

logger = init_logger(__name__)


def prepare_call_dms(
    job_dir,
    region_and_groups,
    base_ds_path,
    codebook_path,
    output_bucket,
    output_prefix,
    input_bucket,
    instance="n2d-standard-64",
    region="us-west1",
    image_id=None,
    disk_size=None,
):
    """Prepare call DMS cloud config."""
    job_dir = pathlib.Path(job_dir)
    job_dir.mkdir(exist_ok=True, parents=True)

    # prepare job config
    records = pd.read_csv(region_and_groups, header=None, names=["region", "group"], index_col=0).squeeze()
    output_prefix = str(output_prefix).rstrip("/")

    for genome_region, group_file in records.items():
        group_file = pathlib.Path(group_file).resolve().absolute()
        chrom, start, end = genome_region.split("-")
        record = {
            "groups_file": group_file,
            "output_bucket": output_bucket,
            "output_prefix": f"{output_prefix}/{genome_region}",
            "base_ds_path": base_ds_path,
            "codebook_path": codebook_path,
            "chrom": chrom,
            "start": start,
            "end": end,
            "groups": group_file.name,
            "cpu": _get_cpu_from_instance_name(instance),
        }

        out_config = job_dir / f"{genome_region}.config.yaml"
        uploaded_config = job_dir / f"{genome_region}.config.yaml_uploaded"
        if uploaded_config.exists():
            continue

        rander_preset_config("call_dms", out_config, **record)

    # prepare sky template
    out_sky = job_dir / "SKY_TEMPLATE.yaml"
    vm_record = {
        "instance": instance,
        "region": region,
        "image_id": image_id,
        "disk_size": disk_size,
        "input_bucket": input_bucket,
    }
    rander_preset_sky("call_dms", out_sky, **vm_record)
    return
