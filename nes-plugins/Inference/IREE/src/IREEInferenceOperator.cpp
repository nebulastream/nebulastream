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

#include "IREEInferenceOperator.hpp"
#include <ranges>
#include <Execution/Operators/ExecutionContext.hpp>
#include <Execution/Operators/Streaming/Join/StreamJoinUtil.hpp>
#include <Functions/NodeFunctionFieldAccess.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalInferModelOperator.hpp>
#include <Util/Common.hpp>
#include <nautilus/function.hpp>

#include <ExecutableOperatorRegistry.hpp>

#include "IREEAdapter.hpp"
#include "IREEInferenceOperatorHandler.hpp"

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}
namespace NES::Runtime::Execution::Operators
{

template <class T>
void addValueToModel(int index, float value, void* inferModelHandler)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter();
    adapter->addModelInput(index, value);
}

void applyModel(void* inferModelHandler)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter();
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter();
    return adapter->getResultAt(index);
}

void IREEInferenceOperator::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const
{
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);

    for (int i = 0; i < inputFieldNames.size(); i++)
    {
        VarVal value = record.read(inputFieldNames.at(nautilus::static_val<int>(i)));
        nautilus::invoke(addValueToModel<float>, nautilus::val<int>(i), value.cast<nautilus::val<float>>(), inferModelHandler);
    }

    nautilus::invoke(applyModel, inferModelHandler);

    for (int i = 0; i < outputFieldNames.size(); i++)
    {
        VarVal result = VarVal(nautilus::invoke(getValueFromModel, nautilus::val<int>(i), inferModelHandler));
        record.write(outputFieldNames.at(i), result);
    }

    child->execute(ctx, record);
}
void IREEInferenceOperator::open(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    ExecutableOperator::open(executionCtx, recordBuffer);
    invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->start(*pec, 0); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
}
void IREEInferenceOperator::close(ExecutionContext& executionCtx, RecordBuffer& recordBuffer) const
{
    invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->stop(QueryTerminationType::Graceful, *pec); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
    ExecutableOperator::close(executionCtx, recordBuffer);
}
}
namespace NES::Runtime::Execution::Operators::ExecutableOperatorGeneratedRegistrar
{

ExecutableOperatorRegistryReturnType RegisterIREEExecutableOperator(ExecutableOperatorRegistryArguments arguments)
{
    if (!NES::Util::instanceOf<QueryCompilation::PhysicalOperators::PhysicalInferModelOperator>(arguments.physical))
    {
        throw UnknownPhysicalOperator("Attempted to lower to IREE, but received: {}", *arguments.physical);
    }
    auto inferModelOperator = NES::Util::as<QueryCompilation::PhysicalOperators::PhysicalInferModelOperator>(arguments.physical);

    NES_INFO("Lower infer model operator to IREE operator");
    auto model = inferModelOperator->getModel();

    // TODO: check model file extension (when it's clear what types are accepted)
    // if (!model.ends_with(".foo"))
    // {
    //     return {};
    // }

    std::vector<std::string> inputFields;
    for (const auto& inputField : inferModelOperator->getInputFields())
    {
        auto nodeFunctionFieldAccess = NES::Util::as<NodeFunctionFieldAccess>(inputField);
        inputFields.push_back(nodeFunctionFieldAccess->getFieldName());
    }

    auto fieldNames = std::views::transform(inferModelOperator->getModel().getOutputs(), [](const auto& pair) { return pair.first; })
        | std::ranges::to<std::vector>();

    auto handler = std::make_shared<IREEInferenceOperatorHandler>(model);
    arguments.operatorHandlers.push_back(handler);

    auto ireeOperator = std::make_shared<IREEInferenceOperator>(arguments.operatorHandlers.size() - 1, inputFields, fieldNames);
    return ireeOperator;
}

}
