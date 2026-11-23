# codon-fuzz: entry=case_entry args=VARSIZED returns=VARSIZED
# codon-fuzz-rows: 0
# codon-fuzz-inputs: [["1,2"], ["100,-24"], ["0,0"], ["-11,-24"], ["123456,789"], [" 7, 8"]]
# real-udf: origin=streaming id=pulsar_custom_object requires=pulsar-client wrapped=1 ported=0
# Wrapper: case_entry runs the message through CustomSerDe.deserialize, CustomObjectFunction.process (context=None) and CustomSerDe.serialize, as the Pulsar runtime does, and returns the serialized text.
# Imports: the original module's imports.

from pulsar import Function, SerDe

# Source: https://github.com/apache/pulsar/blob/f35349f8866cf5398f0259ad5ebe0513c0da497e/pulsar-functions/python-examples/custom_object_function.py#L23-L51
# Repository: https://github.com/apache/pulsar (commit f35349f8866cf5398f0259ad5ebe0513c0da497e)
# License: Apache-2.0
# System: Pulsar Functions
# Copied verbatim (dedented when nested); imports of the original module are not repeated.

class MyObject(object):
  def __init__(self):
    self.a = 0
    self.b = 0


class CustomSerDe(SerDe):
  def __init__(self):
    pass

  def serialize(self, object):
    return ("%d,%d" % (object.a, object.b)).encode('utf-8')

  def deserialize(self, input_bytes):
    split = str(input_bytes.decode()).split(',')
    retval = MyObject()
    retval.a = int(split[0])
    retval.b = int(split[1])
    return retval


class CustomObjectFunction(Function):
  def __init__(self):
    pass

  def process(self, input, context):
    retval = MyObject()
    retval.a = input.a + 11
    retval.b = input.b + 24
    return retval


_custom_serde = CustomSerDe()
_custom_object_function = CustomObjectFunction()


def case_entry(message):
    result = _custom_object_function.process(_custom_serde.deserialize(message.encode("utf-8")), None)
    return _custom_serde.serialize(result).decode("utf-8")
