from pathlib import Path

import jinja2

import gliderport

TEMPLATE_DIR = Path(gliderport.__path__[0]) / "preset/template"


def _get_config_template(job_name):
    config = TEMPLATE_DIR / f"{job_name}.config_template.jinja2"
    if not config.exists():
        raise FileNotFoundError(f"preset {config} not found.")
    return config


def _get_sky_template(job_name):
    config = TEMPLATE_DIR / f"{job_name}.sky_template.jinja2"
    if not config.exists():
        raise FileNotFoundError(f"preset {config} not found.")
    return config


def _rander_jinja2(template_path, **kwargs):
    with open(template_path) as f:
        template = jinja2.Template(f.read())
        content = template.render(kwargs, undefined=jinja2.StrictUndefined)
    return content


def rander_preset_config(name, output_path, **kwargs):
    """Get preset config yaml dict."""
    path = _get_config_template(name)
    content = _rander_jinja2(path, **kwargs)
    with open(output_path, "w") as f:
        f.write(content)
    return


def rander_preset_sky(name, output_path, **kwargs):
    """Get preset sky yaml dict."""
    path = _get_sky_template(name)
    content = _rander_jinja2(path, **kwargs)
    with open(output_path, "w") as f:
        f.write(content)
    return
