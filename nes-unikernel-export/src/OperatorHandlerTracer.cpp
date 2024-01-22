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
#include "Common/DataTypes/DataType.hpp"
#include "Util/Logger/Logger.hpp"
#include <Identifiers.hpp>
#include <OperatorHandlerTracer.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Util/Common.hpp>
#include <Util/magicenum/magic_enum.hpp>
#include <algorithm>
#include <cxxabi.h>
#include <ranges>
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

void OperatorHandlerTracer::reset() {
    delete traceContext;
    traceContext = new OperatorHandlerTracer();
}

static size_t fixAlignment(size_t currentOffset, size_t alignment) {
    size_t remainder = currentOffset % alignment;
    if (remainder == 0) {
        return currentOffset;
    }
    size_t fixedOffset = currentOffset + alignment - remainder;
    NES_ASSERT2_FMT(fixedOffset % alignment == 0, "New offset is not aligned");
    NES_ASSERT2_FMT(fixedOffset > currentOffset, "New offset has to be larger than old offset");
    return fixedOffset;
}

static size_t getBufferSize(const std::vector<OperatorHandlerDescriptor>& handlers) {
    size_t bufferSize = handlers[0].size;
    for (const auto& handler : handlers | std::ranges::views::drop(1)) {
        bufferSize = fixAlignment(bufferSize, handler.alignment);
        bufferSize += handler.size;
    }
    return bufferSize;
}

std::string OperatorHandlerTracer::generateAlignedStaticBufferAllocation(std::vector<OperatorHandlerDescriptor> descriptors) {
    if (descriptors.empty()) {
        return "";
    }

    std::stringstream ss;
    size_t bufferSize = getBufferSize(descriptors);
    size_t alignment = descriptors[0].alignment;

    ss << "template<size_t SizeInBytes>\n"
       << "struct alignas(" << alignment << ") ByteBuffer {\n"
       << "std::array<uint8_t, SizeInBytes> buffer;\n"
       << "void* offset(size_t offset){\n"
       << "return std::addressof(buffer[offset]);\n"
       << "}\n"
       << "};\n";

    ss << "static ByteBuffer<" << bufferSize << "> buffer;\n";

    return ss.str();
};

std::string OperatorHandlerTracer::generateOperatorInstantiation(std::vector<OperatorHandlerDescriptor> descriptors) {
    if (descriptors.empty()) {
        return "";
    }

    size_t bufferSize = getBufferSize(descriptors);
    std::stringstream ss;
    ss << "static std::array<NES::Runtime::Execution::OperatorHandler*, " << descriptors.size() << "> handlers = {" << std::endl;
    ss << descriptors[0].generate(0);
    size_t current = descriptors[0].size;
    for (const auto& desc : descriptors | std::ranges::views::drop(1)) {
        current = fixAlignment(current, desc.alignment);
        ss << ",\n";
        ss << desc.generate(current);
        current += desc.size;
    }
    ss << "};" << std::endl;

    return ss.str();
}

std::string generateSharedHandlerIf(std::vector<EitherSharedOrLocal> descriptors) {
    std::stringstream ss;
    ss << "if (";
    for (const auto& either : descriptors) {
        if (std::holds_alternative<size_t>(either)) {
            ss << "index == " << std::get<size_t>(either) << " && ";
        }
    }
    ss << "true)";

    return ss.str();
}
std::vector<OperatorHandlerDescriptor> OperatorHandlerTracer::getDescriptors() { return get()->descriptors; }
std::string OperatorHandlerTracer::generateFile(std::vector<EitherSharedOrLocal> eitherSharedOrLocal,
                                                uint64_t pipelineID,
                                                NES::QuerySubPlanId subPlanId) {
    std::stringstream ss;

    std::vector<OperatorHandlerDescriptor> descriptors;
    std::vector<size_t> sharedHandlers;
    for (auto& handler : eitherSharedOrLocal) {
        if (std::holds_alternative<OperatorHandlerDescriptor>(handler)) {
            descriptors.emplace_back(std::get<OperatorHandlerDescriptor>(handler));
        } else {
            sharedHandlers.emplace_back(std::get<size_t>(handler));
        }
    }

    ss << generateRuntimeIncludes();

    for (const auto& descriptor : descriptors) {
        ss << descriptor.generateInclude();
    }

    if (!sharedHandlers.empty()) {
        ss << "template<size_t SubQueryId>\n";
        ss << "extern NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler(int index);\n";
    }

    ss << generateAlignedStaticBufferAllocation(descriptors);
    ss << "template<> NES::Runtime::Execution::OperatorHandler* NES::Unikernel::Stage<" << pipelineID
       << ">::getOperatorHandler(int index) { \n"
       << generateOperatorInstantiation(descriptors);
    ss << "switch(index){\n";

    size_t localHandlerIndex = 0;
    for (size_t i = 0; i < eitherSharedOrLocal.size(); ++i) {
        ss << "case " << i << ":\n";
        if (std::holds_alternative<OperatorHandlerDescriptor>(eitherSharedOrLocal[i])) {
            ss << "\treturn handlers[" << localHandlerIndex++ << "];\n";
        } else {
            ss << "\treturn getSharedOperatorHandler<" << subPlanId << ">(" << std::get<size_t>(eitherSharedOrLocal[i]) << ");\n";
        }
    }
    ss << "default:\n"
       << "\t assert(false && \"Index out of bounds for getOperatorHandler\");";
    ss << "}\n";
    ss << "}\n";
    ss << "template<> void NES::Unikernel::Stage<" << pipelineID
       << ">::execute("
          "NES::Unikernel::UnikernelPipelineExecutionContext& context, NES::Runtime::WorkerContext* workerContext, "
          "NES::Runtime::TupleBuffer& buffer) {}\n";
    ss << "template<> void NES::Unikernel::Stage<" << pipelineID
       << ">::setup(NES::Unikernel::UnikernelPipelineExecutionContext& context, NES::Runtime::WorkerContext* workerContext) "
          "{}\n";
    ss << "template<> void NES::Unikernel::Stage<" << pipelineID
       << ">::terminate(NES::Unikernel::UnikernelPipelineExecutionContext& context, NES::Runtime::WorkerContext* "
          "workerContext) {}\n";

    return ss.str();
}

std::string OperatorHandlerTracer::generateRuntimeIncludes() {
    return "#include <OperatorHandler.hpp>\n"
           "#include <UnikernelStage.hpp>\n"
           "#include <cassert>\n"
           "#include <array>\n";
           // "#include <Runtime/RuntimeForwardRefs.hpp>\n";
}

std::string OperatorHandlerTracer::generateSharedHandlerFile(const std::vector<OperatorHandlerDescriptor>& handlers,
                                                             NES::QuerySubPlanId subPlanId) {
    std::stringstream ss;

    ss << generateRuntimeIncludes();

    for (auto& handler : handlers) {
        ss << handler.generateInclude();
    }
    ss << "template<size_t SubQueryPlanId>\nNES::Runtime::Execution::OperatorHandler* "
          "getSharedOperatorHandler(int index);\n";
    ss << generateAlignedStaticBufferAllocation(handlers) << "template <>\n"
       << "NES::Runtime::Execution::OperatorHandler* getSharedOperatorHandler<" << subPlanId
       << ">(int "
          "index) { \n"
       << generateOperatorInstantiation(handlers);
    ss << "assert(index < " << handlers.size() << "&& \"Called getSharedOperatorHandler with an out of bound index\");\n"
       << "return handlers[index];"
       << "}\n";
    return ss.str();
}

OperatorHandlerDescriptor::OperatorHandlerDescriptor(std::string className,
                                                     std::string headerPath,
                                                     size_t classSize,
                                                     size_t alignment,
                                                     std::vector<OperatorHandlerParameterDescriptor> parameters)
    : className(std::move(className)), headerPath(std::move(headerPath)), size(classSize), alignment(alignment),
      parameters(std::move(parameters)) {}

std::string OperatorHandlerDescriptor::generate(size_t offset) const {
    std::stringstream ss;

    ss << "new(buffer.offset(" << offset << "))" << className << "(";

    if (!parameters.empty()) {
        ss << parameters[0].generate();
        for (const auto& param : parameters | std::ranges::views::drop(1)) {
            ss << "," << param.generate();
        }
    }

    ss << ")";

    return ss.str();
}
std::string OperatorHandlerDescriptor::generateInclude() const { return "#include <" + headerPath + ">\n"; }
std::string OperatorHandlerParameterDescriptor::generate() const {
    switch (type) {
        case INT32: return typeName() + "(" + std::to_string(std::any_cast<int32_t>(value)) + ")";
        case UINT32: return typeName() + "(" + std::to_string(std::any_cast<uint32_t>(value)) + ")";
        case INT64: return typeName() + "(" + std::to_string(std::any_cast<int64_t>(value)) + ")";
        case UINT64: return typeName() + "(" + std::to_string(std::any_cast<uint64_t>(value)) + ")";
        case VECTOR: {
            auto vector = std::any_cast<std::vector<OperatorHandlerParameterDescriptor>>(value);
            std::stringstream ss;
            ss << typeName();
            ss << "{";

            if (!vector.empty()) {
                ss << vector[0].generate();
            }

            for (const auto& param : vector | std::ranges::views::drop(1)) {
                ss << ',';
                ss << param.generate();
            }
            ss << "}";
            return ss.str();
        }
        case SHARED_PTR: {
            auto pointeeValue = std::any_cast<OperatorHandlerParameterDescriptor>(value);
            return "std::shared_ptr<" + pointeeValue.typeName() + ">(new " + pointeeValue.generate() + ")";
        }
        case FLOAT32: return typeName() + "(" + std::to_string(std::any_cast<float>(value)) + ")";
        case FLOAT64: return typeName() + "(" + std::to_string(std::any_cast<double>(value)) + ")";
        case ENUM_CONSTANT: return typeName() + "(" + std::to_string(std::any_cast<int8_t>(value)) + ")";
        case SCHEMA: {
            std::stringstream ss;
            auto schema = any_cast<NES::SchemaPtr>(value);
            ss << "NES::Schema::create()";
            for (size_t i = 0; i < schema->getSize(); ++i) {
                ss << "->addField(";
                ss << schema->get(i)->getName();
                ss << ", ";
                ss << schema->get(i)->getDataType()->toString();
                ss << ")";
            }
            return ss.str();
        }
        case BATCH_JOIN_DEFINITION: {
            auto joinDefinition = any_cast<NES::Join::Experimental::LogicalBatchJoinDefinitionPtr>(value);
            std::stringstream ss;

            ss << "NES::Join::Experimental::LogicalBatchJoinDefinition::create(";
            ss << "NES::FieldAccessExpressionNode::create(";
            ss << joinDefinition->getBuildJoinKey()->getStamp()->toString();
            ss << ", ";
            ss << joinDefinition->getBuildJoinKey()->getFieldName();
            ss << "), ";
            ss << "NES::FieldAccessExpressionNode::create(";
            ss << joinDefinition->getProbeJoinKey()->getStamp()->toString();
            ss << ", ";
            ss << joinDefinition->getProbeJoinKey()->getFieldName();
            ss << "), ";
            ss << joinDefinition->getNumberOfInputEdgesBuild();
            ss << ", ";
            ss << joinDefinition->getNumberOfInputEdgesProbe();
            ss << ")";
        }
    }

    NES_THROW_RUNTIME_ERROR("Not implemented for type");
}
std::string OperatorHandlerParameterDescriptor::typeName() const {
    switch (type) {
        case INT32: return "int32_t";
        case UINT32: return "uint32_t";
        case INT64: return "int64_t";
        case UINT64: return "uint64_t";
        case VECTOR: return "std::vector<" + any_cast<std::vector<OperatorHandlerParameterDescriptor>>(value)[0].typeName() + ">";
        case FLOAT32: return "float";
        case FLOAT64: return "double";
        case ENUM_CONSTANT: return "int8_t";
        case SHARED_PTR: return "std::shared_ptr<" + any_cast<OperatorHandlerParameterDescriptor>(value).typeName() + ">";
        case SCHEMA: return "NES::Schema";
        case BATCH_JOIN_DEFINITION: return "NES::Join::Experimental::LogicalBatchJoinDefinition";
    }
}
}// namespace NES::Runtime::Unikernel
