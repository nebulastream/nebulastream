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
#include <Execution/Operators/Streaming/InferModel/InferModelHandler.hpp>
#include <Execution/Operators/Streaming/InferModel/TensorflowAdapter.hpp>
#include <Execution/Operators/Streaming/InferModel/InferModel.hpp>
#include <Nautilus/Interface/FunctionCall.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

void* getTensorflowAdapter(void* op) {
    auto handler = static_cast<InferModelHandler*>(op);
    return handler->getTensorflowAdapter();
}

void InferModel::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const {

    //1. Extract the adapter
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);
    auto tensorflowAdapter = Nautilus::FunctionCall("getTensorflowAdapter", getTensorflowAdapter, inferModelHandler);

    //2. Extract the input vector

    //3. Call the adapter
    tensorflowAdapter.

    //4. Update the record

    //5. Trigger execution of next operator
    child->execute(ctx, (Record&) record);
}

}// namespace NES::Runtime::Execution::Operators
