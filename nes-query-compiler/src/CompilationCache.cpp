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

#include <CompilationCache.hpp>

#include <algorithm>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <sstream>
#include <string>
#include <typeinfo>
#include <unordered_map>
#include <utility>
#include <vector>

#include <DataTypes/Schema.hpp>
#include <Identifiers/Identifiers.hpp>
#include <PhysicalOperator.hpp>
#include <PhysicalPlan.hpp>
#include <Pipeline.hpp>
#include <SinkPhysicalOperator.hpp>
#include <SourcePhysicalOperator.hpp>
#include <options.hpp>

namespace NES::QueryCompilation
{
namespace
{
void appendOperatorSpecificCacheSignature(const PhysicalOperator& physicalOperator, std::ostringstream& signature)
{
    if (const auto source = physicalOperator.tryGet<SourcePhysicalOperator>(); source.has_value())
    {
        const auto descriptor = source->getDescriptor();
        const auto logicalSource = descriptor.getLogicalSource();
        signature << ",sourceType=" << descriptor.getSourceType();
        signature << ",sourceName=" << logicalSource.getLogicalSourceName();
        signature << ",sourceSchema=" << *logicalSource.getSchema();
        const auto parserConfig = descriptor.getParserConfig();
        signature << ",parserType=" << parserConfig.parserType;
        signature << ",tupleDelimiter=" << parserConfig.tupleDelimiter;
        signature << ",fieldDelimiter=" << parserConfig.fieldDelimiter;
    }
    if (const auto sink = physicalOperator.tryGet<SinkPhysicalOperator>(); sink.has_value())
    {
        const auto descriptor = sink->getDescriptor();
        signature << ",sinkType=" << descriptor.getSinkType();
        signature << ",sinkSchema=" << *descriptor.getSchema();
        if (const auto formatType = descriptor.getFormatType(); formatType.has_value())
        {
            signature << ",sinkFormat=" << formatType.value();
        }
        else
        {
            signature << ",sinkFormat=<none>";
        }
    }
}

void appendOptionalSchemaSignature(std::ostringstream& signature, const std::optional<Schema>& schema)
{
    if (schema.has_value())
    {
        signature << schema.value();
    }
    else
    {
        signature << "<none>";
    }
}

void appendOptionalLayoutSignature(std::ostringstream& signature, const std::optional<MemoryLayoutType>& layout)
{
    if (layout.has_value())
    {
        signature << static_cast<int>(layout.value());
    }
    else
    {
        signature << "<none>";
    }
}

void appendWrapperCacheSignature(
    const std::shared_ptr<PhysicalOperatorWrapper>& wrapper,
    std::ostringstream& signature,
    std::unordered_map<const PhysicalOperatorWrapper*, uint64_t>& wrapperOrdinals,
    uint64_t& nextWrapperOrdinal)
{
    const auto wrapperPtr = wrapper.get();
    if (const auto it = wrapperOrdinals.find(wrapperPtr); it != wrapperOrdinals.end())
    {
        signature << "{ref=w" << it->second << "}";
        return;
    }
    const auto wrapperOrdinal = nextWrapperOrdinal++;
    wrapperOrdinals.emplace(wrapperPtr, wrapperOrdinal);

    const auto& physicalOperator = wrapper->getPhysicalOperator();
    signature << "{w=" << wrapperOrdinal;
    signature << ",op=" << physicalOperator.toString();
    appendOperatorSpecificCacheSignature(physicalOperator, signature);
    signature << ",inSchema=";
    appendOptionalSchemaSignature(signature, wrapper->getInputSchema());
    signature << ",outSchema=";
    appendOptionalSchemaSignature(signature, wrapper->getOutputSchema());
    signature << ",inLayout=";
    appendOptionalLayoutSignature(signature, wrapper->getInputMemoryLayoutType());
    signature << ",outLayout=";
    appendOptionalLayoutSignature(signature, wrapper->getOutputMemoryLayoutType());
    signature << ",loc=" << static_cast<int>(wrapper->getPipelineLocation());
    signature << ",children=[";
    for (const auto& children = wrapper->getChildren(); const auto& child : children)
    {
        appendWrapperCacheSignature(child, signature, wrapperOrdinals, nextWrapperOrdinal);
    }
    signature << "]}";
}
}

CompilationCache::CompilationCache(Settings settings) : settings(std::move(settings))
{
}

bool CompilationCache::isEnabled() const
{
    return settings.enabled && !settings.cacheDir.empty();
}

void CompilationCache::prepareForQuery(const PhysicalPlan& physicalPlan)
{
    resetPipelineOrdinals();
    if (!isEnabled())
    {
        cacheKeySeed.clear();
        return;
    }
    cacheKeySeed = createCacheKeySeed(physicalPlan);
}

void CompilationCache::resetPipelineOrdinals()
{
    pipelineToStableOrdinalMap.clear();
    nextStablePipelineOrdinal = 0;
}

uint64_t CompilationCache::getStablePipelineOrdinal(const std::shared_ptr<Pipeline>& pipeline)
{
    const Pipeline* pipelinePtr = pipeline.get();
    if (const auto it = pipelineToStableOrdinalMap.find(pipelinePtr); it != pipelineToStableOrdinalMap.end())
    {
        return it->second;
    }
    const auto ordinal = nextStablePipelineOrdinal++;
    pipelineToStableOrdinalMap.emplace(pipelinePtr, ordinal);
    return ordinal;
}

std::string CompilationCache::createExplicitCacheKey(const std::shared_ptr<Pipeline>& pipeline)
{
    const auto pipelineOrdinal = getStablePipelineOrdinal(pipeline);
    const auto handlerCacheSignature = createHandlerCacheSignature(*pipeline);

    std::ostringstream keyBuilder;
    keyBuilder << "nes:auto";
    if (!cacheKeySeed.empty())
    {
        keyBuilder << ":q=" << cacheKeySeed;
    }
    keyBuilder << ":o=" << pipelineOrdinal;
    keyBuilder << ":h=" << handlerCacheSignature;
    return keyBuilder.str();
}

void CompilationCache::configureEngineOptionsForPipeline(nautilus::engine::Options& options, const std::shared_ptr<Pipeline>& pipeline)
{
    if (!isEnabled())
    {
        return;
    }

    options.setOption("engine.Blob.CacheDir", settings.cacheDir);
    options.setOption("engine.Blob.CacheKey", createExplicitCacheKey(pipeline));
}

std::string CompilationCache::createCacheKeySeed(const PhysicalPlan& physicalPlan)
{
    std::ostringstream signature;
    signature << "physical-plan-sql-canonical";
    const auto& originalSql = physicalPlan.getOriginalSql();
    signature << "|sqlLen=" << originalSql.size();
    signature << "|sql=" << originalSql;
    signature << "|mode=" << static_cast<int>(physicalPlan.getExecutionMode());
    signature << "|buffer=" << physicalPlan.getOperatorBufferSize();
    signature << "|rootCount=" << physicalPlan.getRootOperators().size();
    signature << "|roots=[";
    std::unordered_map<const PhysicalOperatorWrapper*, uint64_t> wrapperOrdinals;
    uint64_t nextWrapperOrdinal = 0;
    for (const auto& rootWrapper : physicalPlan.getRootOperators())
    {
        appendWrapperCacheSignature(rootWrapper, signature, wrapperOrdinals, nextWrapperOrdinal);
    }
    signature << "]";
    return signature.str();
}

std::string CompilationCache::createHandlerCacheSignature(const Pipeline& pipeline)
{
    std::vector<std::pair<uint64_t, std::string>> handlerEntries;
    handlerEntries.reserve(pipeline.getOperatorHandlers().size());
    for (const auto& [handlerId, handler] : pipeline.getOperatorHandlers())
    {
        std::ostringstream entryBuilder;
        entryBuilder << handlerId.getRawValue() << '=';
        if (handler)
        {
            const auto* handlerPtr = handler.get();
            entryBuilder << typeid(*handlerPtr).name();
        }
        else
        {
            entryBuilder << "<null>";
        }
        handlerEntries.emplace_back(handlerId.getRawValue(), entryBuilder.str());
    }

    std::ranges::sort(handlerEntries, [](const auto& lhs, const auto& rhs) { return lhs.first < rhs.first; });

    std::ostringstream signatureBuilder;
    signatureBuilder << "h[";
    for (const auto& handlerEntry : handlerEntries)
    {
        signatureBuilder << handlerEntry.second << ';';
    }
    signatureBuilder << ']';
    return signatureBuilder.str();
}

}
