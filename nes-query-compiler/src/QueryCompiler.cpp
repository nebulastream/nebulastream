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


#include <QueryCompiler.hpp>

#include <CompilationCache.hpp>

#include <optional>
#include <memory>
#include <Configuration/WorkerConfiguration.hpp>
#include <Phases/LowerToCompiledQueryPlanPhase.hpp>
#include <Phases/PipeliningPhase.hpp>
#include <PhysicalOperator.hpp>
#include <Runtime/Execution/OperatorHandler.hpp>
#include <Util/DumpMode.hpp>
#include <CompiledQueryPlan.hpp>
#include <ErrorHandling.hpp>

namespace NES::QueryCompilation
{
namespace
{
struct OperatorHandlerIdAllocatorInfo
{
    std::optional<uint64_t> namespacePrefix;
    std::optional<OperatorHandlerId> maxHandlerId;
};

void collectOperatorHandlerIdAllocatorInfo(
    const std::shared_ptr<PhysicalOperatorWrapper>& wrapper, OperatorHandlerIdAllocatorInfo& allocatorInfo)
{
    if (const auto handlerId = wrapper->getHandlerId(); handlerId.has_value())
    {
        const auto handlerNamespacePrefix = getOperatorHandlerIdNamespacePrefix(handlerId.value());
        if (!allocatorInfo.namespacePrefix.has_value())
        {
            allocatorInfo.namespacePrefix = handlerNamespacePrefix;
        }
        else
        {
            PRECONDITION(
                allocatorInfo.namespacePrefix.value() == handlerNamespacePrefix,
                "All operator handlers of a physical plan must share the same namespace");
        }

        if (!allocatorInfo.maxHandlerId.has_value()
            || getOperatorHandlerIdOrdinal(handlerId.value()) > getOperatorHandlerIdOrdinal(allocatorInfo.maxHandlerId.value()))
        {
            allocatorInfo.maxHandlerId = handlerId.value();
        }
    }

    for (const auto& child : wrapper->getChildren())
    {
        collectOperatorHandlerIdAllocatorInfo(child, allocatorInfo);
    }
}

void initializeOperatorHandlerIdAllocatorForQuery(const PhysicalPlan& physicalPlan)
{
    OperatorHandlerIdAllocatorInfo allocatorInfo;
    for (const auto& root : physicalPlan.getRootOperators())
    {
        collectOperatorHandlerIdAllocatorInfo(root, allocatorInfo);
    }

    if (allocatorInfo.namespacePrefix.has_value())
    {
        initializeOperatorHandlerIdAllocator(allocatorInfo.namespacePrefix.value(), INITIAL_OPERATOR_HANDLER_ID.getRawValue());
        if (allocatorInfo.maxHandlerId.has_value())
        {
            advanceOperatorHandlerIdAllocatorPast(allocatorInfo.maxHandlerId.value());
        }
        return;
    }

    initializeOperatorHandlerIdAllocator(physicalPlan.getOriginalSql());
}
}

QueryCompiler::QueryCompiler() = default;

/// This phase should be as dumb as possible and not further decisions should be made here.
std::unique_ptr<CompiledQueryPlan> QueryCompiler::compileQuery(std::unique_ptr<QueryCompilationRequest> request)
{
    initializeOperatorHandlerIdAllocatorForQuery(request->queryPlan);
    const auto cacheEnabled = request->compilationCacheMode != CompilationCacheMode::Disabled;
    auto compilationCache
        = CompilationCache(CompilationCache::Settings{cacheEnabled, std::move(request->compilationCacheDir)});
    compilationCache.prepareForQuery(request->queryPlan);

    auto lowerToCompiledQueryPlanPhase = LowerToCompiledQueryPlanPhase(request->dumpCompilationResult, &compilationCache);
    auto pipelinedQueryPlan = PipeliningPhase::apply(request->queryPlan);
    return lowerToCompiledQueryPlanPhase.apply(pipelinedQueryPlan);
}
}
