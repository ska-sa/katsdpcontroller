"""Makes packaged JSON schemas available."""

import json
import codecs
from distutils.version import StrictVersion

import pkg_resources
import jsonschema
import jinja2


_env = jinja2.Environment(loader=jinja2.PackageLoader(__name__, '.'))


def _make_validator(schema):
    """Check a schema document and create a validator from it"""
    validator_cls = jsonschema.validators.validator_for(schema)
    validator_cls.check_schema(schema)
    return validator_cls(schema, format_checker=jsonschema.FormatChecker())


class MultiVersionValidator(object):
    """Validation wrapper that supports a versioned schema.

    The schema must have a top-level `version` key. It is defined by a Jinja2
    template that contains two macros:

    - ``validate_version()`` returns a mini version of the schema that only
      validates the version.
    - ``validate(version)`` returns a schema for the given version.

    If the version is a string, then it is converted to a
    :class:`~distutils.version.StrictVersion` before being passed to
    `validate`. The template can thus compare it against strings and get
    sensible version ordering.
    """

    def __init__(self, name):
        self._template = _env.get_template(name)
        # Load the mini-schema that just validates the version
        schema = json.loads(self._template.module.validate_version())
        self._version_validator = _make_validator(schema)

    @staticmethod
    def _get_version(doc):
        version = doc["version"]
        return StrictVersion(version) if isinstance(version, str) else version

    def validate(self, doc):
        self._version_validator.validate(doc)
        version = self._get_version(doc)
        schema = json.loads(self._template.module.validate(version=version))
        validator = _make_validator(schema)
        validator.validate(doc)


for name in pkg_resources.resource_listdir(__name__, '.'):
    if name.endswith('.json'):
        with codecs.getreader('utf-8')(pkg_resources.resource_stream(__name__, name)) as reader:
            schema = json.load(reader)
        globals()[name[:-5].upper()] = _make_validator(schema)
    elif name.endswith('.json.j2'):
        globals()[name[:-8].upper()] = MultiVersionValidator(name)
