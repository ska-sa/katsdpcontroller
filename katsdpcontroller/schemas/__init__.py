"""Makes packaged JSON schemas available."""

import json
import codecs

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
    template which takes a version parameter. It must define an argument-less
    macro called ``validate_version`` that returns a mini version of the schema
    that validates only the version number.
    """

    def __init__(self, name):
        self._template = _env.get_template(name)
        # Load the mini-schema that just validates the version
        schema = json.loads(self._template.module.validate_version())
        self._version_validator = _make_validator(schema)

    def validate(self, doc):
        self._version_validator.validate(doc)
        schema = json.loads(self._template.render(version=doc["version"]))
        validator = _make_validator(schema)
        validator.validate(doc)

    def iter_errors(self, doc):
        try:
            self._version_validator.validate(doc)
        except jsonschema.ValidationError:
            return self._version_validator.iter_errors(doc)
        else:
            schema = json.loads(self._template.render(version=doc["version"]))
            validator = _make_validator(schema)
            return validator.iter_errors(doc)


for name in pkg_resources.resource_listdir(__name__, '.'):
    if name.endswith('.json'):
        reader = codecs.getreader('utf-8')(pkg_resources.resource_stream(__name__, name))
        schema = json.load(reader)
        globals()[name[:-5].upper()] = _make_validator(schema)
    elif name.endswith('.json.j2'):
        globals()[name[:-8].upper()] = MultiVersionValidator(name)
