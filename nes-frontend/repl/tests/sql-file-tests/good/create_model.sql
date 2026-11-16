-- Test CREATE MODEL, SHOW MODELS, and DROP MODEL lifecycle
-- tiny_identity.onnx is a 100-element f32 tensor on each side; VARSIZED mirrors it as a single bulk-byte field.
CREATE MODEL testModel ('tests/testdata/model/tiny_identity.onnx')
INPUT (f1 VARSIZED)
OUTPUT (o1 VARSIZED);
SHOW MODELS;
DROP MODEL WHERE NAME = 'TESTMODEL';
SHOW MODELS;
