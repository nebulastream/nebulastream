# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["hello"], ["Pulsar Functions"], [""], ["sensor-7 temp=21.5"], ["¡hola"]]
# real-udf: origin=streaming id=pulsar_exclamation requires=pulsar-client wrapped=1 ported=0
# Wrapper: case_entry calls ExclamationFunction.process with context=None (the function does not use it).
# Imports: the original module's imports.

from pulsar import Function

# Source: https://github.com/apache/pulsar/blob/f35349f8866cf5398f0259ad5ebe0513c0da497e/pulsar-functions/python-examples/exclamation_function.py#L25-L30
# Repository: https://github.com/apache/pulsar (commit f35349f8866cf5398f0259ad5ebe0513c0da497e)
# License: Apache-2.0
# System: Pulsar Functions
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

class ExclamationFunction(Function):
  def __init__(self):
    pass

  def process(self, input, context):
    return input + '!'


_exclamation_function = ExclamationFunction()


def case_entry(input):
    return _exclamation_function.process(input, None)
