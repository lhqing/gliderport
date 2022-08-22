import click


@click.command("merge-allc")
@click.option("--allc_table_csv", required=True, help="Path to allc CSV table. Tow columns: 1) allc_path 2) group")
@click.option("--chrom_size_cloud_path", required=True, help="Path to chrom size path on cloud.")
@click.option("--job_dir", required=True, help="Path to local job directory, will create if not exists.")
@click.option("--output_bucket", required=True, help="Bucket name for output files.")
@click.option("--output_prefix", required=True, help="Prefix for output files.")
@click.option("--instance", required=False, default="n2d-highcpu-48", help="VM instance name.")
@click.option("--region", required=True, default="us-west1", help="VM region.")
@click.option("--port_idle_hours", required=False, default=100, help="Port idle hours.")
@click.option("--use_hash", required=False, default=None, help="Use a fixed hash for the glider port.")
@click.option("--spot/--no-spot", default=True, help="Use spot VMs.")
def merge_allc(
    allc_table_csv,
    chrom_size_cloud_path,
    job_dir,
    output_bucket,
    output_prefix,
    instance="n2d-highcpu-48",
    region="us-west1",
    port_idle_hours=100,
    n_uploader=1,
    n_worker=16,
    worker_idle_time=600,
    worker_launch_timeout=600,
    use_hash=None,
    spot=True,
):
    """Merge allc files."""
    from gliderport.preset.merge_allc import prepare_merge_allc
    from gliderport.sky import GliderPort

    """Run merge allc job."""
    prepare_merge_allc(
        allc_table_csv=allc_table_csv,
        chrom_size_cloud_path=chrom_size_cloud_path,
        job_dir=job_dir,
        output_bucket=output_bucket,
        output_prefix=output_prefix,
        instance=instance,
        region=region,
    )
    port = GliderPort(
        local_job_dir=job_dir,
        n_uploader=n_uploader,
        n_worker=n_worker,
        max_idle_time=worker_idle_time,
        launch_timeout=worker_launch_timeout,
        use_hash=use_hash,
        spot=spot,
    )
    port.run(max_idle_hours=port_idle_hours)
    return


@click.group()
def glider_preset():
    """Glider port preset command line interface."""
    return


def _glider_preset():
    """Glider port preset command line interface."""
    glider_preset.add_command(merge_allc)
    glider_preset()
    return
