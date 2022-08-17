import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

from google.cloud import storage


def _prepare_file_path(path: str) -> Path:
    return Path(path)


def _prepare_file_paths(file_paths):
    if isinstance(file_paths, str):
        file_paths = [file_paths]

    paths = []
    for path in file_paths:
        path = Path(path).absolute().resolve()
        if not path.exists():
            raise FileNotFoundError(f"File {path} not found")
        paths.append(path)
    return paths


class GCSClient:
    """Get object from GCS."""

    def __init__(self, bucket, prefix):
        self._client = storage.Client()
        self.bucket = self._client.get_bucket(bucket)
        self.prefix = prefix
        self.gcs_location = f"gs://{bucket}/{prefix}"

    def get_obj_list(self):
        """Get object list from GCS."""
        blobs_list = list(self.bucket.list_blobs(prefix=self.prefix))
        return blobs_list

    def get_obj_paths(self):
        """Get object paths from GCS."""
        blobs_list = self.get_obj_list()
        paths = [blob.name for blob in blobs_list]
        return paths


class FileUploader(GCSClient):
    """
    File manager class.

    Take a list of files and a destination location (GCS bucket or local path) and move them to the destination folder
    """

    def __init__(self, file_paths, bucket, prefix, config_file):
        """
        Initialize file manager class.

        Parameters
        ----------
        file_paths :
            List of file paths to be transferred
        bucket :
            GCS bucket name
        prefix :
            GCS prefix for files
        config_file :
            Config file to be transferred
        """
        super().__init__(bucket, prefix)
        self.file_paths = _prepare_file_paths(file_paths)
        self.file_names = [path.name for path in self.file_paths]
        self._temp_file_path = self._make_temp()
        self.config_file = Path(config_file).absolute().resolve()

    def _make_temp(self):
        # create a temp file for file paths
        with NamedTemporaryFile(mode="w", delete=False) as fp:
            for path in self.file_paths:
                fp.write(f"{path}\n")
        return fp.name

    def _move_files(self):
        # move files to destination
        subprocess.run(f"cat {self._temp_file_path} | gsutil -m cp -I {self.gcs_location}", shell=True)

    def _move_config(self):
        # move config file to destination
        subprocess.run(f"gsutil -m cp {self.config_file} {self.gcs_location}/CONFIG", shell=True)

    def _move_file_paths(self):
        subprocess.run(f"gsutil -m cp {self._temp_file_path} {self.gcs_location}/FILE_PATHS", shell=True)

    def _validate_transfer(self):
        gcs_paths = self.get_obj_paths()
        target_paths = [f"{self.prefix}/{name}" for name in self.file_names]

        flag_path = f"{self.prefix}/FILE_PATHS"
        if flag_path not in gcs_paths:
            return False

        config_path = f"{self.prefix}/CONFIG"
        if config_path not in gcs_paths:
            return False

        for path in target_paths:
            if path not in gcs_paths:
                return False
        return True

    def _transfer(self):
        self._move_files()
        self._move_config()
        self._move_file_paths()

    def transfer(self, retry=3, redo=False):
        """Transfer files to GCS."""
        tried = 0

        if redo:
            tried += 1
            self._transfer()
        else:
            if self._validate_transfer():
                print("Files already transferred")
                return

        while not self._validate_transfer():
            tried += 1
            if tried > retry:
                raise Exception(f"Files not transferred after {retry} tries")
            self._transfer()
