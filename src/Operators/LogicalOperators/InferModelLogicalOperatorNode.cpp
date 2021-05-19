/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#include <API/AttributeField.hpp>
#include <Nodes/Expressions/FieldAssignmentExpressionNode.hpp>
#include <Operators/LogicalOperators/InferModelLogicalOperatorNode.hpp>
#include <Optimizer/Utils/QuerySignatureUtil.hpp>
#include "tensorflow/lite/interpreter.h"
#include "tensorflow/lite/kernels/register.h"
#include "tensorflow/lite/model.h"
#include "tensorflow/lite/optional_debug_tools.h"

#define TFLITE_MINIMAL_CHECK(x)                              \
  if (!(x)) {                                                \
    fprintf(stderr, "Error at %s:%d\n", __FILE__, __LINE__); \
    exit(1);                                                 \
  }

namespace NES {

InferModelLogicalOperatorNode::InferModelLogicalOperatorNode(std::string model, std::vector<ExpressionItem> inputFields, std::vector<ExpressionItem> outputFields, OperatorId id)
    : OperatorNode(id), LogicalUnaryOperatorNode(id) {
    this->model = model;
    this->inputFields = inputFields;
    this->outputFields = outputFields;
}

const std::string InferModelLogicalOperatorNode::toString() const {
    std::unique_ptr<tflite::FlatBufferModel> ml_model =
        tflite::FlatBufferModel::BuildFromFile("/home/sumegim/Documents/tub/thesis/tflite/hello_world/iris_92acc.tflite");

    // Build the interpreter with the InterpreterBuilder.
    // Note: all Interpreters should be built with the InterpreterBuilder,
    // which allocates memory for the Interpreter and does various set up
    // tasks so that the Interpreter can read the provided model.
    tflite::ops::builtin::BuiltinOpResolver resolver;
    tflite::InterpreterBuilder builder(*ml_model, resolver);
    std::unique_ptr<tflite::Interpreter> interpreter;
    builder(&interpreter);
    TFLITE_MINIMAL_CHECK(interpreter != nullptr);

    // Allocate tensor buffers.
    TFLITE_MINIMAL_CHECK(interpreter->AllocateTensors() == kTfLiteOk);
    printf("=== Pre-invoke Interpreter State ===\n");
    tflite::PrintInterpreterState(interpreter.get());

    // Fill input buffers
    // TODO(user): Insert code to fill input tensors.
    // Note: The buffer of the input tensor with index `i` of type T can
    // be accessed with `T* input = interpreter->typed_input_tensor<T>(i);`

    float* input = interpreter->typed_input_tensor<float>(0);

    // 5.1, 3.5, 1.4, 0.2 should be 0
    // 6.0, 2.9, 4.5, 1.5 should be 1
    // 6.2, 3.4, 5.4, 2.3 should be 2

//    input[0] = 5.1f;
//    input[1] = 3.5f;
//    input[2] = 1.4f;
//    input[3] = 0.2f;

//    input[0] = 6.0f;
//    input[1] = 2.9f;
//    input[2] = 4.5f;
//    input[3] = 1.5f;
//
    input[0] = 6.2f;
    input[1] = 3.4f;
    input[2] = 5.4f;
    input[3] = 2.3f;

    // Run inference
    TFLITE_MINIMAL_CHECK(interpreter->Invoke() == kTfLiteOk);
    printf("\n\n=== Post-invoke Interpreter State ===\n");
    tflite::PrintInterpreterState(interpreter.get());

    // Read output buffers
    // TODO(user): Insert getting data out code.
    // Note: The buffer of the output tensor with index `i` of type T can
    // be accessed with `T* output = interpreter->typed_output_tensor<T>(i);`

    float* output = interpreter->typed_output_tensor<float>(9);

    std::cout << output[0] << std::endl;
    std::cout << output[1] << std::endl;
    std::cout << output[2] << std::endl;

    std::stringstream ss;
    ss << "Model: " << model << std::endl;
    ss << "input fields:" << std::endl;
    for (auto v : inputFields){
        ss << "    " << v.getExpressionNode()->toString() << std::endl;
    }
    ss << "output fields:" << std::endl;
    for (auto v : outputFields){
        ss << "    " << v.getExpressionNode()->toString() << std::endl;
    }
    return ss.str();
}

OperatorNodePtr InferModelLogicalOperatorNode::copy() {
    auto copy = LogicalOperatorFactory::createInferModelOperator(model, inputFields, outputFields, id);
    return copy;
}
bool InferModelLogicalOperatorNode::equal(const NodePtr rhs) const {
    if (rhs->instanceOf<InferModelLogicalOperatorNode>()) {
        auto inferModelOperator = rhs->as<InferModelLogicalOperatorNode>();
        return model == inferModelOperator->model;
    }
    return false;
}

bool InferModelLogicalOperatorNode::isIdentical(NodePtr rhs) const {
    return equal(rhs) && rhs->as<InferModelLogicalOperatorNode>()->getId() == id;
}

bool InferModelLogicalOperatorNode::inferSchema() {
    if (!LogicalUnaryOperatorNode::inferSchema()) {
        return false;
    }
    return true;
}
std::string InferModelLogicalOperatorNode::getStringBasedSignature() {
    return toString();
}

}// namespace NES
