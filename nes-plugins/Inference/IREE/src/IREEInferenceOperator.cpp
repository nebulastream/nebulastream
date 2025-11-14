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

#include <ExecutionContext.hpp>
#include <IREEAdapter.hpp>
#include <IREEInferenceOperator.hpp>
#include <IREEInferenceOperatorHandler.hpp>
#include <Nautilus/Interface/Record.hpp>
#include <QueryExecutionConfiguration.hpp>
#include <nautilus/function.hpp>
#include <ranges>

namespace NES::QueryCompilation::PhysicalOperators
{
class PhysicalInferModelOperator;
}


namespace NES::IREEInference
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
}

namespace NES
{

void IREEInferenceOperator::execute(ExecutionContext& ctx, NES::Nautilus::Record& record) const
{
    auto inferModelHandler = ctx.getGlobalOperatorHandler(inferModelHandlerIndex);

    if (!this->isVarSizedInput)
    {
        for (nautilus::static_val<size_t> i = 0; i < inputs.size(); ++i)
        {
            nautilus::invoke(
                IREEInference::addValueToModel<float>,
                nautilus::val<int>(i),
                inputs.at(i).execute(record, ctx.pipelineMemoryProvider.arena).cast<nautilus::val<float>>(),
                inferModelHandler,
                ctx.workerThreadId);
        }
    }
    else
    {
        VarVal value = inputs.at(0).execute(record, ctx.pipelineMemoryProvider.arena);
        auto varSizedValue = value.cast<VariableSizedData>();
        nautilus::invoke(
            IREEInference::copyVarSizedToModel,
            varSizedValue.getContent(),
            IREEInference::min(varSizedValue.getContentSize(), nautilus::val<uint32_t>(static_cast<uint32_t>(this->inputSize))),
            inferModelHandler,
            ctx.workerThreadId);
    }

    nautilus::invoke(IREEInference::applyModel, inferModelHandler, ctx.workerThreadId);

    if (!this->isVarSizedOutput)
    {
        for (nautilus::static_val<size_t> i = 0; i < outputFieldNames.size(); ++i)
        {
            VarVal result = VarVal(nautilus::invoke(IREEInference::getValueFromModel, nautilus::val<int>(i), inferModelHandler, ctx.workerThreadId));
            record.write(outputFieldNames.at(i), result);
        }
    }
    else
    {
        auto output = ctx.pipelineMemoryProvider.arena.allocateVariableSizedData(this->outputSize);
        nautilus::invoke(IREEInference::copyVarSizedFromModel, output.getContent(), output.getContentSize(), inferModelHandler, ctx.workerThreadId);
        record.write(outputFieldNames.at(0), output);
    }

    child->execute(ctx, record);
}

void IREEInferenceOperator::setup(ExecutionContext& executionCtx, CompilationContext&) const
{
    nautilus::invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->start(*pec, 0); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
}

void IREEInferenceOperator::terminate(ExecutionContext& executionCtx) const
{
    nautilus::invoke(
        +[](OperatorHandler* opHandler, PipelineExecutionContext* pec) { opHandler->stop(QueryTerminationType::Graceful, *pec); },
        executionCtx.getGlobalOperatorHandler(inferModelHandlerIndex),
        executionCtx.pipelineContext);
}
}
