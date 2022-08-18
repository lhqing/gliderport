import click

from gliderport.files import FileUploader
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
@click.option("--config_file", required=True, help="Config file to be transferred")
@click.option("--redo/--no-redo", default=False, help="Redo the upload if the file already exists")
def upload_files(bucket, prefix, file_paths, file_list_path=None, config_file=None, redo=False):
    """Upload files to GCS."""
    if file_paths is None and file_list_path is None:
        raise ValueError("Either file_paths or file_list_path must be provided")

    file_uploader = FileUploader(
        bucket=bucket, prefix=prefix, file_paths=file_paths, file_list_path=file_list_path, config_file=config_file
    )
    file_uploader.transfer(redo=redo)
    return


@click.command("vm-worker")
@click.option("--bucket", required=True, help="GCS bucket name")
@click.option("--prefix", required=True, help="GCS prefix for job config files")
@click.option("--max_idle_time", required=False, default=600, help="Max idle time in seconds")
def vm_worker(bucket, prefix, max_idle_time):
    """Run a worker on VM."""
    Worker(job_bucket=bucket, job_prefix=prefix, max_idle_time=max_idle_time)
    return


@click.group()
def glider():
    """Glider port command line interface."""
    return


def main():
    """Glider port command line interface."""
    glider.add_command(upload_files)
    glider.add_command(vm_worker)
    glider()
    return
