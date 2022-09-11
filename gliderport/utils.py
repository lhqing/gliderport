import re
import subprocess
import time

import yaml

from .log import init_logger

logger = init_logger(__name__)


def read_config(config_file, input_mode=None):
    """
    Read config file and return config dict and input option.

    Job config file format created by user and GliderPort:
    - input: 1) local file list; 2) bucket and prefix to get files from GCS
    - output (optional): bucked and prefix to upload files to GCS
    - delete_input (optional): True or False, whether to delete the input files on source GCS after the job is done
    - run: multiline commands or a list of commands to run on the VM to generate the output files
    - python: multiline python scripts or a list of python scripts to run on the VM to generate the output files
    """
    with open(config_file) as fp:
        config = yaml.load(fp, Loader=yaml.FullLoader)

    assert "input" in config, f"Input is not defined in config.\n{config}"

    if "local" in config["input"]:
        input_opt = "local"
    elif "bucket" in config["input"] and "prefix" in config["input"]:
        input_opt = "gcs"
    else:
        input_opt = None

    if input_opt is None:
        raise ValueError(
            f"Input is not properly defined in config. " f"It should be either local or bucket and prefix.\n{config}"
        )
    if input_mode is not None:
        if input_opt != input_mode:
            raise ValueError(f"Input is not in {input_mode} form.\n{config}")

    # output is optional now
    # assert "output" in config, f"Output is not defined in config.\n{config}"
    # assert "bucket" in config["output"], f"Output bucket is not defined in config.\n{config}"
    # assert "prefix" in config["output"], f"Output prefix is not defined in config.\n{config}"

    flag = False
    for key in ["run", "python"]:
        if key in config:
            flag = True
    if not flag:
        raise ValueError(f"Run is not defined in config.\n{config}")
    return config, input_opt


class CommandRunner:
    """Run command using subprocess.run()."""

    def __init__(self, command, log_prefix, check=False, retry=2, env=None, sleep_after_fail=0):
        self.command = command
        self.log_prefix = log_prefix
        self.check = check
        self.env = env
        self.retry = retry
        self.sleep_after_fail = sleep_after_fail

    def _run(self, run_id):
        try:
            logger.info(f"Running command: {self.command}")
            p = subprocess.run(
                self.command, shell=True, capture_output=True, check=True, encoding="utf-8", env=self.env
            )
            self._save_info(p, run_id)
            return True
        except subprocess.CalledProcessError as e:
            logger.error(f"Command failed\n{self.command}")
            logger.error(e.output)
            logger.error(e.stderr)
            self._save_info(e, run_id)
            return False

    def _save_info(self, obj, run_id):
        if self.log_prefix is None:
            return
        log_prefix = str(self.log_prefix)
        stdout_path = f"{log_prefix}.stdout_{run_id}.log"
        stderr_path = f"{log_prefix}.stderr_{run_id}.log"

        if log_prefix.startswith("gs://"):
            import gcsfs

            fs = gcsfs.GCSFileSystem()
            with fs.open(stdout_path, "w") as fp:
                fp.write(obj.stdout)
            with fs.open(stderr_path, "w") as fp:
                fp.write(obj.stderr)
        else:
            with open(stdout_path, "w") as f:
                f.write(obj.stdout)
            with open(stderr_path, "w") as f:
                f.write(obj.stderr)
        return

    def run(self):
        """Run command and return True if success."""
        success = False
        for i in range(self.retry):
            flag = self._run(run_id=i)
            if flag:
                success = True
                break
        if not success and self.check:
            if self.log_prefix is not None:
                logger.error(f"Check logs at {self.log_prefix}.std*.log")
            time.sleep(self.sleep_after_fail)
            raise ValueError(f"Failed to run command {self.command} after {self.retry} retries.")
        return success


def validate_name(name):
    """Validate name or fail early."""
    p = re.compile("[a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?")
    m = p.match(name)
    if m is None:
        flag = False
    else:
        start, end = m.span()
        if start != 0 or end != len(name):
            flag = False
        else:
            flag = True

    if not flag:
        raise ValueError(
            f"Name '{name}' is invalid; ensure it is fully matched " "by regex: [a-z0-9]([-a-z0-9]{0,61}[a-z0-9])?"
        )
    return
