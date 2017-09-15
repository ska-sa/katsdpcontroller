"""Makes packaged JSON schemas available."""

from __future__ import print_function, division, absolute_import
import json

import pkg_resources
import jsonschema


for name in pkg_resources.resource_listdir(__name__, '.'):
    if name.endswith('.json'):
        schema = json.load(pkg_resources.resource_stream(__name__, name))
        validator_cls = jsonschema.validators.validator_for(schema)
        validator_cls.check_schema(schema)
        validator = validator_cls(schema, format_checker=jsonschema.FormatChecker())
        globals()[name[:-5].upper()] = validator
