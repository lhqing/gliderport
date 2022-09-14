import click

from gliderport.files import FileUploader
from gliderport.sky import GliderPort
from gliderport.vm import Worker


@click.command("upload")
@click.option("--bucket", required=True, help="GCS bucket name")
@click.option("--prefix", required=True, help="GCS prefix for files")
@click.option("--file_paths", required=False, default=None, multiple=True, help="List of file paths to be transferred")
@click.option(
    "--file_list_path",
    required=False,
    default=None,
    help="A single file with each row containing a file path to be transferred",
)
@click.option("--redo/--no-redo", default=False, help="Redo the upload if the file already exists")
@click.option("--parallel/--no-parallel", default=True, help="Use parallel -m option in gsutil")
def upload_files(bucket, prefix, file_paths, file_list_path=None, redo=False, parallel=True):
    """Upload files to cloud."""
    if file_paths is None and file_list_path is None:
        raise ValueError("Either file_paths or file_list_path must be provided")

    file_uploader = FileUploader(
        bucket=bucket, prefix=prefix, file_paths=file_paths, file_list_path=file_list_path, parallel=parallel
    )
    file_uploader.transfer(redo=redo)
    return


@click.command("vm-worker")
@click.option("--bucket", required=True, help="GCS bucket name")
@click.option("--prefix", required=True, help="GCS prefix for job config files")
@click.option("--max_idle_time", required=False, default=1200, help="Max idle time in seconds")
@click.option("--cleanup/--no-cleanup", default=True, help="Cleanup local files before run")
def vm_worker(bucket, prefix, max_idle_time, cleanup):
    """Run a worker on VM."""
    Worker(job_bucket=bucket, job_prefix=prefix, max_idle_time=max_idle_time, cleanup_before_run=cleanup)
    return


@click.command("port")
@click.option("--local_job_dir", required=True, help="Local job directory")
@click.option("--n_uploader", required=False, default=1, help="Number of uploaders")
@click.option("--n_worker", required=False, default=16, help="Number of workers")
@click.option("--max_idle_hours", required=False, default=100, help="Max idle hours for glider port to wait")
@click.option("--use_hash", required=False, default=None, help="Unique port and worker hash for the spot job name")
@click.option("--parallel/--no-parallel", default=True, help="Use parallel -m option in gsutil")
@click.option(
    "--pre_submit", required=False, default=2, help="number of jobs per worker to submit before the first worker is up"
)
def port(local_job_dir, n_uploader=1, n_worker=16, max_idle_hours=100, use_hash=None, parallel=True, pre_submit=2):
    """Run a glider port on-prem."""
    gp = GliderPort(
        local_job_dir=local_job_dir, n_uploader=n_uploader, n_worker=n_worker, use_hash=use_hash, pre_submit=pre_submit
    )
    gp.run(max_idle_hours=max_idle_hours, gsutil_parallel=parallel)
    return


@click.command("warm-up")
@click.option("--region", required=True, help="GCP region")
@click.option("--spot/--no-spot", default=True, help="Use spot instances")
def _warm_up(region, spot):
    """Warm up sky by submit an empty job."""
    from .preset.utilities import warm_up_sky

    warm_up_sky(region=region, spot=spot)
    return


@click.group()
def glider():
    """Glider port command line interface."""
    return


def _glider():
    """Glider port command line interface."""
    glider.add_command(upload_files)
    glider.add_command(vm_worker)
    glider.add_command(port)
    glider.add_command(_warm_up)
    glider()
    return
