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

#include "API/AttributeField.hpp"
#include "Util/Logger/Logger.hpp"
#include <Identifiers.hpp>
#include <OperatorHandlerTracer.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <Util/Common.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <cxxabi.h>
#include <ranges>
namespace NES::Join::Experimental {
class AbstractBatchJoinHandler;
class LogicalBatchJoinDefinition;
}// namespace NES::Join::Experimental
namespace NES::Runtime::Unikernel {
static thread_local OperatorHandlerTracer* traceContext = nullptr;

template<>
OperatorHandlerParameterDescriptor OperatorHandlerParameterDescriptor::of(std::shared_ptr<NES::Schema> const& value) {
    return {OperatorHandlerParameterType::SCHEMA, value};
}

template<>
OperatorHandlerParameterDescriptor
OperatorHandlerParameterDescriptor::of(std::shared_ptr<NES::Join::Experimental::LogicalBatchJoinDefinition> const& value) {
    return {OperatorHandlerParameterType::BATCH_JOIN_DEFINITION, value};
}

template<>
OperatorHandlerParameterDescriptor
OperatorHandlerParameterDescriptor::of(const std::shared_ptr<NES::Join::Experimental::AbstractBatchJoinHandler>& value) {
    NES_THROW_RUNTIME_ERROR("I haven't yet thought about how this is handled!");
}
template<>
OperatorHandlerParameterDescriptor
OperatorHandlerParameterDescriptor::of(const NES::QueryCompilation::StreamJoinStrategy& value) {
    return {OperatorHandlerParameterType::ENUM_CONSTANT, static_cast<int8_t>(value)};
}
template<>
OperatorHandlerParameterDescriptor OperatorHandlerParameterDescriptor::of(const unsigned long& value) {
    return {OperatorHandlerParameterType::UINT64, value};
}
OperatorHandlerTracer* OperatorHandlerTracer::get() {
    NES_ASSERT(traceContext != nullptr, "TraceContext not initialized!");
    return traceContext;
}

GlobalOperatorHandlerIndex OperatorHandlerTracer::currentHandlerId = 0;

void OperatorHandlerTracer::reset() {
    delete traceContext;
    NES_INFO("OperatorHandler Tracer: Reset");
    currentHandlerId = 0;
    traceContext = new OperatorHandlerTracer();
}

std::vector<OperatorHandlerDescriptor> OperatorHandlerTracer::getDescriptors() { return get()->descriptors; }

OperatorHandlerDescriptor::OperatorHandlerDescriptor(std::string className,
                                                     std::string headerPath,
                                                     size_t classSize,
                                                     size_t alignment,
                                                     std::vector<OperatorHandlerParameterDescriptor> parameters)
    : className(std::move(className)), headerPath(std::move(headerPath)), size(classSize), alignment(alignment),
      parameters(std::move(parameters)) {
    NES_INFO("OperatorHandler Tracer: Created Handler {}", OperatorHandlerTracer::currentHandlerId);
    handlerId = OperatorHandlerTracer::currentHandlerId++;
}

}// namespace NES::Runtime::Unikernel
