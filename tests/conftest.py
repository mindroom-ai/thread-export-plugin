# ruff: noqa: INP001
"""Bootstrap the thread-export plugin package for standalone pytest runs."""

from __future__ import annotations

import sys
from importlib import util
from pathlib import Path
from types import ModuleType

PLUGIN_ROOT = Path(__file__).resolve().parent.parent
PACKAGE_NAME = f"mindroom_plugin_{PLUGIN_ROOT.name.replace('-', '_')}"

package = sys.modules.get(PACKAGE_NAME)
if not isinstance(package, ModuleType):
    package = ModuleType(PACKAGE_NAME)
    sys.modules[PACKAGE_NAME] = package

init_path = PLUGIN_ROOT / "__init__.py"
if init_path.is_file():
    package.__file__ = str(init_path)
else:
    package.__dict__.pop("__file__", None)
package.__package__ = PACKAGE_NAME
package.__path__ = [str(PLUGIN_ROOT)]
spec = util.spec_from_loader(PACKAGE_NAME, loader=None, is_package=True)
if spec is not None:
    spec.submodule_search_locations = [str(PLUGIN_ROOT)]
package.__spec__ = spec
