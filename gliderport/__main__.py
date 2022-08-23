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
    """Upload files to GCS."""
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
def vm_worker(bucket, prefix, max_idle_time):
    """Run a worker on VM."""
    Worker(job_bucket=bucket, job_prefix=prefix, max_idle_time=max_idle_time)
    return


@click.command("port")
@click.option("--local_job_dir", required=True, help="Local job directory")
@click.option("--n_uploader", required=False, default=1, help="Number of uploaders")
@click.option("--n_worker", required=False, default=16, help="Number of workers")
@click.option("--max_idle_hours", required=False, default=100, help="Max idle hours for glider port to wait")
@click.option("--parallel/--no-parallel", default=True, help="Use parallel -m option in gsutil")
def port(local_job_dir, n_uploader=1, n_worker=16, max_idle_hours=100, parallel=True):
    """Run a glider port."""
    gp = GliderPort(local_job_dir=local_job_dir, n_uploader=n_uploader, n_worker=n_worker)
    gp.run(max_idle_hours=max_idle_hours, gsutil_parallel=parallel)
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
    glider()
    return
