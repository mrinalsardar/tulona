from typing import Any, Dict

RUNTIME_DEFAULTS = {
    "ENGINE": "pandas",
    "OUTDIR": "output",
}


class RunConfig:
    def __init__(self, options: Dict[str, Any], project: Dict[str, Any]) -> None:
        options = {k: v for k, v in options.items() if v is not None}

        def get_final_value(key):
            return options.get(key, project.get(key, RUNTIME_DEFAULTS[key.upper()]))

        self.engine = get_final_value("engine")
        self.outdir = get_final_value("outdir")
