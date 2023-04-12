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
#include <Execution/Operators/Streaming/InferModel/InferModel.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModelHandler.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

template<class T>
void addValueToModel(T value, void* inferModelHandler) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->addModelInput(value);
}

void applyModel(void* inferModelHandler) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler) {
    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    return adapter->getResultAt(index);
}

void InferModel::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const {

    //1. Extract the handler
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);

    //2. Add input values for the model inference
    for (const auto& inputFieldName : inputFieldNames) {
        Value<> value = record.read(inputFieldName);
        if (value->isType<Int32>()) {
            FunctionCall("addValueToModel", addValueToModel<Int32>, value.as<Int32>(), inferModelHandler);
        } else if (value->isType<Boolean>()) {
            FunctionCall("addValueToModel", addValueToModel<Boolean>, value.as<Boolean>(), inferModelHandler);
        } else if (value->isType<Float>()) {
            FunctionCall("addValueToModel", addValueToModel<Float>, value.as<Float>(), inferModelHandler);
        } else if (value->isType<Double>()) {
            FunctionCall("addValueToModel", addValueToModel<Double>, value.as<Double>(), inferModelHandler);
        } else {
            NES_ERROR2("Can not handle inputs other than of type int, bool, float, and double");
        }
    }

    //3. infer model on the input values
    Nautilus::FunctionCall("applyModel", applyModel, inferModelHandler);

    //4. Get inferred output from the adapter
    for (uint32_t i = 0; i < outputFieldNames.size(); i++) {
        auto value = FunctionCall("getValueFromModel", getValueFromModel, Value<UInt32>(i), inferModelHandler);
        record.write(outputFieldNames.at(i), Value<Float>(value));
    }

    //4. Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

}// namespace NES::Runtime::Execution::Operators
