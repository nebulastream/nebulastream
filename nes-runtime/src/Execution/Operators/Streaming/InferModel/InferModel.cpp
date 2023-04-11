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

#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModel.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModelHandler.hpp>
#include <Execution/Operators/Streaming/InferModel/TensorflowAdapter.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {


void applyModel(void* inferModelHandler, void* inputFields, void* outputFields, void* record) {

    auto castedInputFields = static_cast<std::vector<ExpressionItemPtr>*>(inputFields);
    auto castedRecord = static_cast<NES::Nautilus::Record*>(record);

    std::vector<Value<>> modelInput;
    for (const auto& inputField : *castedInputFields) {
        modelInput.push_back(castedRecord->read(""));
    }

    auto handler = static_cast<InferModelHandler*>(inferModelHandler);
    auto adapter = handler->getTensorflowAdapter();
    adapter->infer(BasicPhysicalType::NativeType::UINT_32, modelInput);

    auto castedOutputFields = static_cast<std::vector<ExpressionItemPtr>*>(outputFields);

    for (const auto& outputField : *castedOutputFields) {
        castedRecord->write("", adapter->getResultAt(0));
    }
}

void InferModel::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const {

    std::vector<Value<>> modelInput;

    //1. Extract the input vector
    //2. Extract and call the adapter
    //3. Update the record
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);
    Nautilus::FunctionCall("applyModel",
                           applyModel,
                           inferModelHandler,
                           Value<MemRef>((int8_t*) &inputFields),
                           Value<MemRef>((int8_t*) &outputFields),
                           Value<MemRef>((int8_t*) &record));

    //4. Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

}// namespace NES::Runtime::Execution::Operators
