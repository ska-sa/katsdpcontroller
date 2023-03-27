"""Makes packaged JSON schemas available."""

import codecs
import json

import jinja2
import jsonschema
import pkg_resources
from packaging.version import Version

_env = jinja2.Environment(loader=jinja2.PackageLoader(__name__, "."))


def _normalise_version(version):
    """Coerce strings to ComparableVersion."""
    return ComparableVersion(version) if isinstance(version, str) else version


def _make_validator(schema):
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

    - ``versions()`` returns a JSON list of supported versions.
    - ``validate(version)`` returns a schema for the given version.

    Versions can be strings or integers, and strings are converted to
    :class:`ComparableVersion` before being passed to `validate`. The template
    can thus compare the version against strings and get sensible version
    ordering.
    """

    def __init__(self, name):
        self._template = _env.get_template(name)
        # Create the mini-schema that just validates the version
        versions = json.loads(self._template.module.versions())
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
        schema = json.loads(self._template.module.validate(version=version))
        validator = _make_validator(schema)
        validator.validate(doc)


for name in pkg_resources.resource_listdir(__name__, "."):
    if name.endswith(".json"):
        with codecs.getreader("utf-8")(pkg_resources.resource_stream(__name__, name)) as reader:
            schema = json.load(reader)
        globals()[name[:-5].upper()] = _make_validator(schema)
    elif name.endswith(".json.j2"):
        globals()[name[:-8].upper()] = MultiVersionValidator(name)
