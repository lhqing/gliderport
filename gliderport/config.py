"""
Config file created by user and GliderPort.

Format of job config:
- input: 1) local file list; 2) bucket and prefix to get files from GCS
- output: bucked and prefix to upload files to GCS
- run: multiline commands or a list of commands to run on the VM to generate the output files
- delete_input (optional): True or False, whether to delete the input files on source GCS after the job is done
"""

import yaml


def read_config(config_file, input_mode=None):
    """Read config file and return config dict and input option."""
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

    assert "output" in config, f"Output is not defined in config.\n{config}"
    assert "bucket" in config["output"], f"Output bucket is not defined in config.\n{config}"
    assert "prefix" in config["output"], f"Output prefix is not defined in config.\n{config}"

    assert "run" in config, f"Worker is not defined in config.\n{config}"
    return config, input_opt
