import pathlib
import tempfile

import jinja2

from ..utils import CommandRunner
from .template import TEMPLATE_DIR


def _get_cpu_from_instance_name(instance_name):
    return int(instance_name.split("-")[-1])


def warm_up_sky(region="us-west1", spot=True):
    """Warm up sky by submit an empty job."""
    config = TEMPLATE_DIR / "warm_up.sky.jinja2"
    with open(config) as f:
        template = jinja2.Template(f.read())
        content = template.render(region=region, undefined=jinja2.StrictUndefined)

        with tempfile.NamedTemporaryFile(mode="w") as f:
            f.write(content)
            f.flush()

            if spot:
                spot_str = "spot"
            else:
                raise NotImplementedError

            cmd = f"sky {spot_str} launch -d -y -n warmup {f.name}"
            log_prefix = pathlib.Path().absolute() / "warm_up_sky"
            CommandRunner(cmd, log_prefix=log_prefix, check=True, retry=2, env=None).run()
    return
