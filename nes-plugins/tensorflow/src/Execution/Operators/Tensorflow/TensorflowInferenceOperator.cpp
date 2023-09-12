/*
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

#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Tensorflow/TensorflowAdapter.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperator.hpp>
#include <Execution/Operators/Tensorflow/TensorflowInferenceOperatorHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

template<class T>
void addValueToModel(int index, T value, void* inferModelHandler) {
    auto handler = static_cast<TensorflowInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->addModelInput<T>(index, value);
}

void applyModel(void* inferModelHandler) {
    auto handler = static_cast<TensorflowInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler) {
    auto handler = static_cast<TensorflowInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    return adapter->getResultAt(index);
}

void TensorflowInferenceOperator::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const {

    //1. Extract the handler
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);

    //2. Add input values for the model inference
    for (uint32_t i = 0; i < inputFieldNames.size(); i++) {
        Value<> value = record.read(inputFieldNames.at(i));
        if (value->isType<Boolean>()) {
            FunctionCall("addValueToModel", addValueToModel<bool>, Value<UInt32>(i), value.as<Boolean>(), inferModelHandler);
        } else if (value->isType<Float>()) {
            FunctionCall("addValueToModel", addValueToModel<float>, Value<UInt32>(i), value.as<Float>(), inferModelHandler);
        } else if (value->isType<Double>()) {
            FunctionCall("addValueToModel", addValueToModel<double>, Value<UInt32>(i), value.as<Double>(), inferModelHandler);
        } else if (value->isType<UInt64>()) {
            FunctionCall("addValueToModel", addValueToModel<uint64_t>, Value<UInt32>(i), value.as<UInt64>(), inferModelHandler);
        } else if (value->isType<UInt32>()) {
            FunctionCall("addValueToModel", addValueToModel<uint32_t>, Value<UInt32>(i), value.as<UInt32>(), inferModelHandler);
        } else if (value->isType<UInt16>()) {
            FunctionCall("addValueToModel", addValueToModel<uint16_t>, Value<UInt32>(i), value.as<UInt16>(), inferModelHandler);
        } else if (value->isType<UInt8>()) {
            FunctionCall("addValueToModel", addValueToModel<uint8_t>, Value<UInt32>(i), value.as<UInt8>(), inferModelHandler);
        } else if (value->isType<Int64>()) {
            FunctionCall("addValueToModel", addValueToModel<int64_t>, Value<UInt32>(i), value.as<Int64>(), inferModelHandler);
        } else if (value->isType<Int32>()) {
            FunctionCall("addValueToModel", addValueToModel<int32_t>, Value<UInt32>(i), value.as<Int32>(), inferModelHandler);
        } else if (value->isType<Int16>()) {
            FunctionCall("addValueToModel", addValueToModel<int16_t>, Value<UInt32>(i), value.as<Int16>(), inferModelHandler);
        } else if (value->isType<Int8>()) {
            FunctionCall("addValueToModel", addValueToModel<int8_t>, Value<UInt32>(i), value.as<Int8>(), inferModelHandler);
        } else {
            NES_ERROR("Can not handle inputs other than of type int, bool, float, and double");
        }
    }

    //3. infer model on the input values
    Nautilus::FunctionCall("applyModel", applyModel, inferModelHandler);

    //4. Get inferred output from the adapter
    for (uint32_t i = 0; i < outputFieldNames.size(); i++) {
        Value<> value = FunctionCall("getValueFromModel", getValueFromModel, Value<UInt32>(i), inferModelHandler);
        record.write(outputFieldNames.at(i), value);
    }

    //4. Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

}// namespace NES::Runtime::Execution::Operators
