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

#include "Nautilus/Interface/DataTypes/Text/Text.hpp"
#include <Execution/Expressions/Expression.hpp>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModelHandler.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModelOperator.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <cstdint>
#include <ranges>
#include <string>

namespace NES::Runtime::Execution::Operators {

void applyModel(void* inferModelHandler) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->infer();
}

template<typename T>
void addValueToModel(void* inferModelHandler, const T& data) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->appendToByteArray(data);
}

void addBase64ValueToModel(void* inferModelHandler, TextValue* data) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->appendBase64EncodedData(data->view());
}

float getValueFromModel(int index, void* inferModelHandler) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    return adapter->getResultAt(index);
}

TextValue* getBase64ValueFromModel(void* inferModelHandler) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    return TextValue::create(adapter->getBase64EncodedData());
}

void InferModelOperator::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const {
    //1. Extract the handler
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);
    auto to_values = std::ranges::views::transform([&record](const auto& name) {
        return record.read(name);
    });

    //2. Add input values for the model inference
    for (auto value : inputFieldNames | to_values) {
        if (value->isType<Float>()) {
            FunctionCall("addValueToModel", addValueToModel<float>, inferModelHandler, value.as<Float>());
        } else if (value->isType<Double>()) {
            FunctionCall("addValueToModel", addValueToModel<double>, inferModelHandler, value.as<Double>());
        } else if (value->isType<UInt64>()) {
            FunctionCall("addValueToModel", addValueToModel<uint64_t>, inferModelHandler, value.as<UInt64>());
        } else if (value->isType<UInt32>()) {
            FunctionCall("addValueToModel", addValueToModel<uint32_t>, inferModelHandler, value.as<UInt32>());
        } else if (value->isType<UInt16>()) {
            FunctionCall("addValueToModel", addValueToModel<uint16_t>, inferModelHandler, value.as<UInt16>());
        } else if (value->isType<UInt8>()) {
            FunctionCall("addValueToModel", addValueToModel<uint8_t>, inferModelHandler, value.as<UInt8>());
        } else if (value->isType<Int64>()) {
            FunctionCall("addValueToModel", addValueToModel<int64_t>, inferModelHandler, value.as<Int64>());
        } else if (value->isType<Int32>()) {
            FunctionCall("addValueToModel", addValueToModel<int32_t>, inferModelHandler, value.as<Int32>());
        } else if (value->isType<Int16>()) {
            FunctionCall("addValueToModel", addValueToModel<int16_t>, inferModelHandler, value.as<Int16>());
        } else if (value->isType<Int8>()) {
            FunctionCall("addValueToModel", addValueToModel<int8_t>, inferModelHandler, value.as<Int8>());
        } else if (value->isType<Nautilus::Text>()) {
            FunctionCall<>("addBase64ValueToModel",
                           addBase64ValueToModel,
                           inferModelHandler,
                           value.as<Text>().value->getReference());
        } else {
            NES_ERROR("Can not handle inputs other than of type int, bool, float, and double");
        }
    }

    //3. infer model on the input values
    Nautilus::FunctionCall("applyModel", applyModel, inferModelHandler);

    if (outputFieldNames.size() == 1 && outputFieldNames.at(0) == "data") {
        //4.1 Get the output tensor as base64
        Value<> value = FunctionCall("getBase64ValueFromModel", getBase64ValueFromModel, inferModelHandler);
        record.write("data", value);
    } else {
        //4.2 Get inferred output from the adapter as floats
        for (uint32_t i = 0; i < outputFieldNames.size(); i++) {
            Value<> value = FunctionCall("getValueFromModel", getValueFromModel, Value<UInt32>(i), inferModelHandler);
            record.write(outputFieldNames.at(i), value);
        }
    }

    //4. Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

}// namespace NES::Runtime::Execution::Operators
