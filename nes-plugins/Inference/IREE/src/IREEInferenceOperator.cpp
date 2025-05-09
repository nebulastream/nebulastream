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
#include <Common/DataTypes/VariableSizedDataType.hpp>
#include "IREEAdapter.hpp"
#include "IREEInferenceOperatorHandler.hpp"

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}
namespace NES::Runtime::Execution::Operators
{

template <class T>
void addValueToModel(int index, float value, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(index, value);
}

void copyVarSizedFromModel(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->copyResultTo(std::span{content, size});
}

void copyVarSizedToModel(std::byte* content, uint32_t size, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->addModelInput(std::span{content, size});
}

void applyModel(void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    adapter->infer();
}

float getValueFromModel(int index, void* inferModelHandler, WorkerThreadId thread)
{
    auto handler = static_cast<IREEInferenceOperatorHandler*>(inferModelHandler);
    auto adapter = handler->getIREEAdapter(thread);
    return adapter->getResultAt(index);
}
template <typename T>
nautilus::val<T> min(const nautilus::val<T>& lhs, const nautilus::val<T>& rhs)
{
    return lhs < rhs ? lhs : rhs;
}

void IREEInferenceOperator::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const
{
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);

    if (!this->isVarSizedInput)
    {
        for (nautilus::static_val<size_t> i = 0; i < inputFieldNames.size(); ++i)
        {
            VarVal value = record.read(inputFieldNames.at(nautilus::static_val<int>(i)));
            nautilus::invoke(
                addValueToModel<float>,
                nautilus::val<int>(i),
                value.cast<nautilus::val<float>>(),
                inferModelHandler,
                ctx.getWorkerThreadId());
        }
    }
    else
    {
        VarVal value = record.read(inputFieldNames.at(nautilus::static_val<int>(0)));
        auto varSizedValue = value.cast<VariableSizedData>();
        nautilus::invoke(
            copyVarSizedToModel,
            varSizedValue.getContent(),
            min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
            inferModelHandler,
            ctx.getWorkerThreadId());
    }

    nautilus::invoke(applyModel, inferModelHandler, ctx.getWorkerThreadId());

    if (!this->isVarSizedOutput)
    {
        for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
        {
            VarVal result = VarVal(nautilus::invoke(getValueFromModel, nautilus::val<int>(i), inferModelHandler, ctx.getWorkerThreadId()));
            record.write(outputFieldNames.at(i), result);
        }
    }
    else
    {
        auto output = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
        nautilus::invoke(copyVarSizedFromModel, output.getContent(), output.getContentSize(), inferModelHandler, ctx.getWorkerThreadId());
        record.write(outputFieldNames.at(0), output);
    }

    child->execute(ctx, record);
}

void IREEInferenceOperator::setup(ExecutionContext& executionCtx) const
{
    ExecutableOperator::setup(executionCtx);
    invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->start(*pec, 0); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
}

void IREEInferenceOperator::terminate(ExecutionContext& executionCtx) const
{
    invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->stop(QueryTerminationType::Graceful, *pec); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
    ExecutableOperator::terminate(executionCtx);
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

    if (inputFields.size() == 1
        && NES::Util::instanceOf<VariableSizedDataType>(
            inferModelOperator->getInputSchema()->getFieldByName(inputFields[0]).value()->getDataType()))
    {
        ireeOperator->isVarSizedInput = true;
    }

    if (inferModelOperator->getOutputFields().size() == 1
        && NES::Util::instanceOf<VariableSizedDataType>(
            inferModelOperator->getOutputSchema()->getFieldByName(inferModelOperator->getOutputFields().at(0)).value()->getDataType()))
    {
        ireeOperator->isVarSizedOutput = true;
    }

    ireeOperator->outputSize = model.outputSize();
    ireeOperator->inputSize = model.inputSize();

    return ireeOperator;
}

}
