################################################################################
# Copyright (c) 2013-2023, National Research Foundation (SARAO)
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use
# this file except in compliance with the License. You may obtain a copy
# of the License at
#
#   https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

"""Makes packaged JSON schemas available.

The schemas themselves are written in YAML, but restricted to the JSON data
types. YAML is used purely for better readability.
"""

from typing import TYPE_CHECKING, Union

import importlib_resources
import jinja2
import jsonschema
import yaml
from packaging.version import Version

_env = jinja2.Environment(loader=jinja2.PackageLoader(__name__, "."))


def _normalise_version(version: Union["ComparableVersion", str]) -> "ComparableVersion":
    """Coerce strings to ComparableVersion."""
    return ComparableVersion(version) if isinstance(version, str) else version


def _make_validator(schema) -> jsonschema.protocols.Validator:
    """Check a schema document and create a validator from it"""
    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls.check_schema(schema)
    return validator_cls(schema, format_checker=jsonschema.FormatChecker())


class ComparableVersion(Version):
    """Version that can be compared to versions represented as strings."""

    def __lt__(self, other) -> bool:
        return super().__lt__(_normalise_version(other))

    def __gt__(self, other) -> bool:
        return super().__gt__(_normalise_version(other))

    def __le__(self, other) -> bool:
        return super().__le__(_normalise_version(other))

    def __ge__(self, other) -> bool:
        return super().__ge__(_normalise_version(other))

    def __eq__(self, other) -> bool:
        return super().__eq__(_normalise_version(other))

    def __ne__(self, other) -> bool:
        return super().__ne__(_normalise_version(other))


class MultiVersionValidator:
    """Validation wrapper that supports a versioned schema.

    The schema must have a top-level `version` key. It is defined by a Jinja2
    template that contains two macros:

    - ``versions()`` returns a YAML list of supported versions.
    - ``validate(version)`` returns a schema for the given version, as YAML.

    Versions can be strings or integers, and strings are converted to
    :class:`ComparableVersion` before being passed to `validate`. The template
    can thus compare the version against strings and get sensible version
    ordering.
    """

    def __init__(self, name):
        self._template = _env.get_template(name)
        # Create the mini-schema that just validates the version
        versions = yaml.safe_load(self._template.module.versions())
        assert isinstance(versions, list)
        schema = {
            "$schema": "http://json-schema.org/draft-07/schema#",
            "type": "object",
            "required": ["version"],
            "properties": {"version": {"enum": versions}},
        }
        self._version_validator = _make_validator(schema)
        self.versions = [_normalise_version(v) for v in versions]

    @staticmethod
    def _get_version(doc):
        return _normalise_version(doc["version"])

    def validate(self, doc):
        self._version_validator.validate(doc)
        version = self._get_version(doc)
        schema = yaml.safe_load(self._template.module.validate(version=version))
        validator = _make_validator(schema)
        validator.validate(doc)


for entry in importlib_resources.files(__name__).iterdir():
    name = entry.name
    if name.endswith(".yaml"):
        schema = yaml.safe_load(entry.read_text())
        globals()[name[:-5].upper()] = _make_validator(schema)
    elif name.endswith(".yaml.j2"):
        globals()[name[:-8].upper()] = MultiVersionValidator(name)

if TYPE_CHECKING:
    # Let type checkers know about the defined schemas
    GPUS: jsonschema.protocols.Validator
    INFINIBAND_DEVICES: jsonschema.protocols.Validator
    INTERFACES: jsonschema.protocols.Validator
    NUMA: jsonschema.protocols.Validator
    PRODUCT_CONFIG: MultiVersionValidator
    S3_CONFIG: jsonschema.protocols.Validator
    STREAMS: jsonschema.protocols.Validator
    SUBSYSTEMS: jsonschema.protocols.Validator
    VOLUMES: jsonschema.protocols.Validator
