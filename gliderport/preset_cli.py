import click


@click.command("merge-allc")
@click.option("--allc_table_csv", required=True, help="Path to allc CSV table. Tow columns: 1) allc_path 2) group")
@click.option("--chrom_size_cloud_path", required=True, help="Path to chrom size path on cloud.")
@click.option("--job_dir", required=True, help="Path to local job directory, will create if not exists.")
@click.option("--output_bucket", required=True, help="Bucket name for output files.")
@click.option("--output_prefix", required=True, help="Prefix for output files.")
@click.option("--merge_allc_cpu", required=False, default=48, help="CPU for merge allc job.")
@click.option("--instance", required=False, default="n2d-standard-48", help="VM instance name.")
@click.option("--region", required=False, default="us-west1", help="VM region.")
@click.option("--port_idle_hours", required=False, default=100, help="Port idle hours.")
@click.option("--use_hash", required=False, default=None, help="Use a fixed hash for the glider port.")
@click.option("--spot/--no-spot", default=True, help="Use spot VMs.")
def merge_allc(
    allc_table_csv,
    chrom_size_cloud_path,
    job_dir,
    output_bucket,
    output_prefix,
    merge_allc_cpu=48,
    instance="n2d-standard-48",
    region="us-west1",
    port_idle_hours=100,
    use_hash=None,
    spot=True,
    n_uploader=1,
    n_worker=16,
    worker_idle_time=600,
    worker_launch_timeout=600,
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
        merge_allc_cpu=merge_allc_cpu,
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


@click.command("papermill")
@click.option("--input_path", required=True, help="Path to input notebook.")
@click.option("--output_path", required=True, help="Path to output notebook.")
@click.option("--config_path", required=True, help="Path to parameters YAML file.")
@click.option("--cwd", required=True, help="Path to current working directory.")
@click.option("--log_path", required=True, help="Path to log file.")
@click.option("--success_flag", required=True, help="Path to success flag file.")
def papermill_special(input_path, output_path, cwd, config_path, log_path, success_flag):
    """Run papermill and ignore error if flag set."""
    import pathlib
    import subprocess

    try:
        subprocess.run(
            "papermill "
            f"--cwd {cwd} "
            f"-f {config_path} "
            f"{input_path} "
            f"{output_path} "
            f"> {log_path} 2>&1 "
            f"&& touch {success_flag}",
            shell=True,
            check=True,
        )
    except subprocess.CalledProcessError as e:
        ignore_path = pathlib.Path(cwd) / "ignore_error"
        if ignore_path.exists():
            print("Error ignored.")
        else:
            raise e
    return


@click.command("mapping")
def mapping():
    """Mapping command line interface."""
    return


@click.group()
def glider_preset():
    """Glider port preset command line interface."""
    return


def _glider_preset():
    """Glider port preset command line interface."""
    glider_preset.add_command(merge_allc)
    glider_preset.add_command(mapping)
    glider_preset.add_command(papermill_special)
    glider_preset()
    return
