import subprocess
from pathlib import Path
from tempfile import NamedTemporaryFile

from google.cloud import storage

from .log import init_logger

logger = init_logger(__name__)


class GCSClient:
    """Get object from GCS."""

    def __init__(self, bucket, prefix):
        self._client = storage.Client()
        self.bucket = self._client.get_bucket(bucket)
        self.prefix = prefix
        self.gcs_location = f"gs://{bucket}/{prefix}"
        self.file_paths = []

    def get_obj_list(self):
        """Get object list from GCS."""
        blobs_list = list(self.bucket.list_blobs(prefix=self.prefix))
        return blobs_list

    def get_obj_paths(self):
        """Get object paths from GCS."""
        blobs_list = self.get_obj_list()
        paths = [blob.name for blob in blobs_list]
        return paths

    @classmethod
    def _make_temp(cls, file_list):
        """Create a temp file for file paths."""
        with NamedTemporaryFile(mode="w", delete=False) as fp:
            for path in file_list:
                fp.write(f"{path}\n")
        return fp.name

    @classmethod
    def _run(cls, cmd):
        try:
            logger.info("Running command:", cmd)
            subprocess.run(cmd, shell=True, capture_output=True, check=True, encoding="utf-8")
        except subprocess.CalledProcessError as e:
            logger.error(e.output)
            logger.error(e.stderr)
            raise e

    @classmethod
    def _move_files(cls, file_list, destination_path):
        # move multiple files to destination
        temp_name = cls._make_temp(file_list)
        cmd = f'cat "{temp_name}" | gsutil -m cp -r -I "{destination_path}"'
        cls._run(cmd)
        return temp_name

    @classmethod
    def _move_file_and_dir(cls, file_path, destination_path):
        # move file to destination
        cmd = f'gsutil -m cp -r "{file_path}" "{destination_path}"'
        cls._run(cmd)

    def add_file_paths(self, *args):
        """Add file paths to list."""
        _file_paths = []
        for path in args:
            if isinstance(path, (str, Path)):
                _file_paths.append(path)
            elif isinstance(path, list):
                _file_paths.extend(path)
            else:
                _file_paths.extend(list(path))

        for path in _file_paths:
            path = Path(path).absolute().resolve()
            if not path.exists():
                # make sure path is not wildcard
                if "*" not in str(path):
                    raise FileNotFoundError(f"File {path} not found")
            self.file_paths.append(path)
        return

    def _transfer(self):
        raise NotImplementedError

    def _validate_transfer(self):
        raise NotImplementedError

    def transfer(self, retry=3, redo=False):
        """Transfer files to GCS."""
        tried = 0

        if redo:
            tried += 1
            self._transfer()
        else:
            if self._validate_transfer():
                logger.info("Files already transferred")
                return

        while not self._validate_transfer():
            tried += 1
            if tried > retry:
                raise Exception(f"Files not transferred after {retry} tries")
            self._transfer()


class FileUploader(GCSClient):
    """
    File manager class.

    Take a list of files and a destination location (GCS bucket or local path) and move them to the destination folder
    """

    def __init__(self, bucket, prefix, file_paths, file_list_path=None):
        """
        Initialize file manager class.

        Parameters
        ----------
        bucket :
            GCS bucket name
        prefix :
            GCS prefix for files
        file_paths :
            List of file paths to be transferred
        file_list_path :
            A single file with each row containing a file path to be transferred
        """
        super().__init__(bucket, prefix)
        if file_list_path is not None:
            file_paths = []
            with open(file_list_path) as fp:
                for line in fp:
                    file_paths.append(line.strip())

        self.add_file_paths(file_paths)
        self.file_names = [path.name for path in self.file_paths]

        self.upload_success_path = f"{self.prefix}/UPLOAD_SUCCESS"

    def _validate_transfer(self):
        gcs_paths = self.get_obj_paths()

        flag_path = f"{self.prefix}/UPLOAD_SUCCESS"
        if flag_path not in gcs_paths:
            return False
        return True

    def _transfer(self):
        file_list_temp_path = self._move_files(self.file_paths, self.gcs_location)
        self._move_file_and_dir(file_list_temp_path, f"{self.gcs_location}/UPLOAD_SUCCESS")


class FileDownloader(GCSClient):
    """Download files from GCS."""

    def __init__(self, bucket, prefix, dest_path):
        super().__init__(bucket, prefix)
        self.dest_path = Path(dest_path).absolute().resolve()
        self.dest_path.mkdir(parents=True, exist_ok=True)
        self.gcs_location = f"gs://{bucket}/{prefix}"
        self.download_success_path = self.dest_path / "DOWNLOAD_SUCCESS"
        self.upload_success_path = self.dest_path / "UPLOAD_SUCCESS"

    def _transfer(self):
        location_wildcard = f"{self.gcs_location}/*"
        self._move_file_and_dir(location_wildcard, self.dest_path)

        # delete upload success flag
        try:
            self.upload_success_path.unlink()
        except FileNotFoundError:
            pass

        # create download success flag
        logger.info("Creating download success flag")
        self.download_success_path.touch()

    def _validate_transfer(self):
        return self.download_success_path.exists()

    def delete_source(self):
        """Delete source files from GCS."""
        cmd = f"gsutil -m rm -r {self.gcs_location}"
        self._run(cmd)
