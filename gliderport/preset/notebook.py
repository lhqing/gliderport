import pathlib
import re

import nbformat as nbf
import pandas as pd
import yaml


def _extract_nb_parameters(nb_path, **default_kwargs):
    ntbk = nbf.read(nb_path, nbf.NO_CONVERT)
    parameters = default_kwargs
    for cell in ntbk.cells:
        tags = cell["metadata"].get("tags", [])
        if "parameters" in tags:
            # execute parameters cell code, and put the variables into parameters
            exec(cell.source, None, parameters)
            break
    return parameters


def _parse_workdir_notebooks(notebook_dir, default_cpu=1, default_mem_gb=1):
    # notebook name pattern
    nb_name_pattern = re.compile(r"(?P<num_step>\d+)(?P<sub_step>\w*).*ipynb")
    # example names:
    # 01.step1.ipynb
    # 02a.step2a.ipynb
    # 02b.any_name.ipynb
    # 03.ipynb  # or no name

    nb_records = {}
    for path in pathlib.Path(notebook_dir).glob("*.ipynb"):
        try:
            match = nb_name_pattern.match(path.name).groupdict()
            nb_records[(int(match["num_step"]), match["sub_step"])] = {
                "path": path.resolve().absolute(),
                "parameters": _extract_nb_parameters(path, cpu=default_cpu, mem_gb=default_mem_gb),
            }
        except AttributeError:
            # not match, ignore this notebook
            continue

    nb_records = pd.DataFrame(nb_records).T.reset_index()
    nb_records.columns = ["num_step", "sub_step"] + nb_records.columns[2:].tolist()
    nb_records = nb_records.loc[:, ["num_step", "sub_step", "path", "parameters"]].copy()
    return nb_records


def notebook_snakemake(work_dir, notebook_dir, groups, default_cpu=1, default_mem_gb=1, redo_prepare=False):
    """
    Prepare snakemake file for running a series of notebooks in multiple groups.

    Parameters
    ----------
    work_dir :
        Working directory for snakemake.
    notebook_dir :
        Directory containing a series of template notebooks with name suffix indicating the execution order.
    groups :
        A list of groups, each group will have its own sub-dir in the work_dir, and run through the notebook series.
        If a single file path is given, each row in the file will be treated as a group.
    default_cpu :
        Default cpu for each notebook.
    default_mem_gb :
        Default memory for each notebook.
    redo_prepare :
        If True, will re-generate the snakemake file even if it already exists.

    """
    work_dir = pathlib.Path(work_dir).resolve().absolute()
    work_dir.mkdir(exist_ok=True, parents=True)

    work_dir = str(work_dir).rstrip("/")

    snakefile_path = pathlib.Path(work_dir) / "Snakefile"
    if snakefile_path.exists() and not redo_prepare:
        print(f"Snakefile already exists at {snakefile_path}, skip.")
        return

    if isinstance(groups, (str, pathlib.Path)):
        with open(groups) as f:
            groups = [line.strip() for line in f]

    nb_records = _parse_workdir_notebooks(
        notebook_dir=notebook_dir, default_cpu=default_cpu, default_mem_gb=default_mem_gb
    )

    rule_template = """

rule {rule_name}:
    input:
        {input_pattern},
    params:
        input_nb="{nb_path}"
    output:
        "{work_dir}/{{group_name}}/log/{rule_name}.success"
    log:
        output_nb="{work_dir}/{{group_name}}/{nb_name}",
        log="{work_dir}/{{group_name}}/log/{nb_name}.log"
    threads:
        {cpu}
    resources:
        mem_gb={mem_gb}
    shell:
        "glider-preset papermill "
        "--input_path {{params.input_nb}} "
        "--output_path {{log.output_nb}} "
        "--config_path {work_dir}/{{wildcards.group_name}}/log/{rule_name}.config.yaml "
        "--cwd {work_dir}/{{wildcards.group_name}} "
        "--log_path {{log.log}} "
        "--success_flag {{output}} "
"""

    snakemake_str = ""
    # notebook rules
    previous_rule_names = None
    rule_name = None
    for num_step, sub_df in nb_records.groupby("num_step"):
        step_rule_names = []
        for _, row in sub_df.iterrows():
            _, sub_step, nb_path, parameters = row

            # save parameters
            cpu = parameters.get("cpu", default_cpu)
            mem_gb = parameters.get("mem_gb", default_mem_gb)

            rule_name = f"step_{num_step}{sub_step}"
            nb_name = pathlib.Path(nb_path).name
            if previous_rule_names is None:
                input_pattern = f'"{snakefile_path}"'
            else:
                input_files = []
                for _rn in previous_rule_names:
                    input_files.append(f'"{work_dir}/{{group_name}}/log/{_rn}.success"')
                input_pattern = ", ".join(input_files)
            step_rule_names.append(rule_name)

            rule_str = rule_template.format(
                rule_name=rule_name,
                input_pattern=input_pattern,
                nb_path=nb_path,
                nb_name=nb_name,
                cpu=cpu,
                mem_gb=mem_gb,
                work_dir=work_dir,
            )
            snakemake_str += rule_str
        previous_rule_names = step_rule_names

    # summary rule
    final_rule_str = f"""
groups = {groups}


rule final:
    input:
        # target the final rule success flag of each group
        expand('{work_dir}/{{group_name}}/log/{rule_name}.success', group_name=groups)
    output:
        "{work_dir}/Snakemake.success"
    shell:
        # create a final success flag
        "touch {{output}}"
"""
    snakemake_str = final_rule_str + snakemake_str

    with open(snakefile_path, "w") as f:
        f.write(snakemake_str)

    # write parameters
    for group in groups:
        group_log_dir = pathlib.Path(f"{work_dir}/{group}/log")
        group_log_dir.mkdir(exist_ok=True, parents=True)

        for _, (num_step, sub_step, _, parameters) in nb_records.iterrows():
            parameters["group_name"] = group
            config_path = group_log_dir / f"step_{num_step}{sub_step}.config.yaml"
            with open(config_path, "w") as f:
                yaml.dump(parameters, f)
    return
