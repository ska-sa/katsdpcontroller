"""Makes packaged JSON schemas available."""

import json
import codecs

import pkg_resources
import jsonschema


for name in pkg_resources.resource_listdir(__name__, '.'):
    if name.endswith('.json'):
        reader = codecs.getreader('utf-8')(pkg_resources.resource_stream(__name__, name))
        schema = json.load(reader)
        validator_cls = jsonschema.validators.validator_for(schema)
        validator_cls.check_schema(schema)
        validator = validator_cls(schema, format_checker=jsonschema.FormatChecker())
        globals()[name[:-5].upper()] = validator
