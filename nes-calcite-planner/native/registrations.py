#!/usr/bin/env python3
"""Writes native-image registrations that tracing queries cannot be relied on to find:

- every Calcite metadata handler: CachingMetadataHandlerProvider calls the RelMd* handler methods reflectively through
  proxies of the BuiltInMetadata interfaces, and which of them a query needs depends on its operators;
- every generated protobuf message (Substrait, our service, protobuf's well-known types), whose accessors protobuf finds
  by reflection on some paths, e.g. for large plans;
- Calcite's RexImpTable implementors and error messages (Resources), both created reflectively, the field trimmer,
  decorrelator and convertlets, which dispatch by reflection, and the parsers, whose error messages reflect on them;
- our resource files and Substrait's extension catalog.

Rerun after a Calcite, Substrait or protobuf upgrade:
    registrations.py target/nes-calcite-planner.jar src/main/resources/META-INF/native-image/nes
"""
import json
import re
import sys
import zipfile
from pathlib import Path

classes = [n[:-len('.class')].replace('/', '.') for n in zipfile.ZipFile(sys.argv[1]).namelist() if n.endswith('.class')]
out = Path(sys.argv[2])


def write(directory: str, reflection: list[dict]) -> None:
    (out / directory).mkdir(parents=True, exist_ok=True)
    (out / directory / 'reachability-metadata.json').write_text(json.dumps({'reflection': reflection}, indent=2) + '\n')


def everything(names: list[str]) -> list[dict]:
    return [{'type': n, 'allDeclaredConstructors': True, 'allPublicConstructors': True, 'allPublicMethods': True,
             'allDeclaredMethods': True, 'allDeclaredFields': True} for n in names]


handlers = sorted(n for n in classes if re.fullmatch(r'org\.apache\.calcite\.rel\.metadata\.RelMd\w+', n))
interfaces = sorted(n for n in classes if re.fullmatch(r'org\.apache\.calcite\.rel\.metadata\.BuiltInMetadata\$\w+(\$Handler)?', n))
write('calcite-metadata-handlers', everything(handlers + interfaces) + [{'type': {'proxy': [n]}} for n in interfaces])

messages = sorted(n for n in classes if re.match(r'(io\.substrait\.proto|nes\.calcite\.proto)\.', n)
                  or re.fullmatch(r'com\.google\.protobuf\.(Any|Empty|Struct|Value|ListValue|Timestamp|Duration|\w+Value)(\$\w+)*', n))
write('protobuf-messages', everything(messages))

# RexImpTable (code generation of Calcite's own execution) is initialized on rare paths, e.g. by RexUtil.EXECUTOR, and
# creates its implementors reflectively.
implementors = sorted(n for n in classes if n.startswith('org.apache.calcite.adapter.enumerable.RexImpTable$')
                      # Calcite's error messages (e.g. validation errors) are created reflectively, too.
                      or n.startswith('org.apache.calcite.runtime.Resources$')
                      or (n.startswith('org.apache.calcite.') and n.endswith('Exception'))
                      # dispatch to a method per node class by reflection (ReflectUtil.createMethodDispatcher)
                      or re.fullmatch(r'org\.apache\.calcite\.sql2rel\.(RelFieldTrimmer|RelDecorrelator)(\$\w+)*', n)
                      # the convertlets of SqlToRel (e.g. for EXTRACT), which ReflectiveConvertletTable finds by name
                      or re.fullmatch(r'org\.apache\.calcite\.sql2rel\.(Standard|Reflective)ConvertletTable(\$\w+)*', n)
                      # our trimmer, which RelFieldTrimmer's dispatch calls by reflection
                      or n == 'nes.calcite.NesFieldTrimmer'
                      # the parser's error messages list the expected tokens by reflecting on its production methods
                      or re.fullmatch(r'org\.apache\.calcite\.sql\.parser\.(ddl\.SqlDdlParserImpl|impl\.SqlParserImpl)(\$\w+)*', n))
write('calcite-reflective', everything(implementors))

# Our own resources, which the native image only contains if listed.
(out / 'nes-resources').mkdir(parents=True, exist_ok=True)
(out / 'nes-resources' / 'reachability-metadata.json').write_text(json.dumps(
    {'resources': [{'glob': 'nes_unsigned_integers.yaml'}, {'glob': 'nes_functions.yaml'}, {'glob': 'substrait/extensions/*.yaml'}]}, indent=2) + '\n')
print(f'{len(handlers)} metadata handlers, {len(interfaces)} metadata interfaces, {len(messages)} protobuf classes, '
      f'{len(implementors)} implementors')
