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

#include <API/AttributeField.hpp>
#include <Common/DataTypes/DataType.hpp>
#include <Operators/Expressions/FieldAccessExpressionNode.hpp>
#include <Operators/LogicalOperators/LogicalBatchJoinDefinition.hpp>
#include <OperatorHandlerTracer.hpp>
#include <Stage/OperatorHandlerCppExport.hpp>
#include <Stage/QueryPipeliner.hpp>
#include <algorithm>

namespace NES::Unikernel::Export {
void OperatorHandlerCppExporter::HandlerExport::runtimeIncludes(const std::vector<std::string>& includePaths) {
    for (const auto& includePath : includePaths) {
        ss << "#include <" << includePath << ">\n";
    }
}
std::string
OperatorHandlerCppExporter::HandlerExport::typeName(const Runtime::Unikernel::OperatorHandlerParameterDescriptor& parameter) {
    auto value = parameter.value;
    switch (parameter.type) {
        case Runtime::Unikernel::OperatorHandlerParameterType::INT32: return "int32_t";
        case Runtime::Unikernel::OperatorHandlerParameterType::UINT32: return "uint32_t";
        case Runtime::Unikernel::OperatorHandlerParameterType::INT64: return "int64_t";
        case Runtime::Unikernel::OperatorHandlerParameterType::UINT64: return "uint64_t";
        case Runtime::Unikernel::OperatorHandlerParameterType::VECTOR:
            return "std::vector<"
                + typeName(std::any_cast<std::vector<Runtime::Unikernel::OperatorHandlerParameterDescriptor>>(value)[0]) + ">";
        case Runtime::Unikernel::OperatorHandlerParameterType::FLOAT32: return "float";
        case Runtime::Unikernel::OperatorHandlerParameterType::FLOAT64: return "double";
        case Runtime::Unikernel::OperatorHandlerParameterType::ENUM_CONSTANT: return "int8_t";
        case Runtime::Unikernel::OperatorHandlerParameterType::SHARED_PTR:
            return "std::shared_ptr<" + typeName(std::any_cast<Runtime::Unikernel::OperatorHandlerParameterDescriptor>(value))
                + ">";
        case Runtime::Unikernel::OperatorHandlerParameterType::SCHEMA: return "NES::Schema";
        case Runtime::Unikernel::OperatorHandlerParameterType::BATCH_JOIN_DEFINITION:
            return "NES::Join::Experimental::LogicalBatchJoinDefinition";
        default: NES_THROW_RUNTIME_ERROR("Not implemented");
    }
}
void OperatorHandlerCppExporter::HandlerExport::generateParameter(
    const Runtime::Unikernel::OperatorHandlerParameterDescriptor& parameter) {
    using NES::Runtime::Unikernel::OperatorHandlerParameterType;
    auto value = parameter.value;
    auto type = parameter.type;
    switch (parameter.type) {
        case OperatorHandlerParameterType::INT32:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<int32_t>(value)) + ")";
            break;
        case OperatorHandlerParameterType::UINT32:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<uint32_t>(value)) + ")";
            break;
        case OperatorHandlerParameterType::INT64:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<int64_t>(value)) + ")";
            break;
        case OperatorHandlerParameterType::UINT64:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<uint64_t>(value)) + ")";
            break;
        case OperatorHandlerParameterType::VECTOR: {
            auto vector = std::any_cast<std::vector<Runtime::Unikernel::OperatorHandlerParameterDescriptor>>(value);
            ss << typeName(parameter);
            ss << "{";

            if (!vector.empty()) {
                generateParameter(vector[0]);
            }

            for (const auto& param : vector | std::ranges::views::drop(1)) {
                ss << ',';
                generateParameter(param);
            }
            ss << "}";
            break;
        }
        case OperatorHandlerParameterType::SHARED_PTR: {
            auto pointeeValue = std::any_cast<Runtime::Unikernel::OperatorHandlerParameterDescriptor>(value);
            ss << "std::shared_ptr<" + typeName(pointeeValue) + ">(new ";
            generateParameter(pointeeValue);
            ss << ")";
            break;
        }
        case OperatorHandlerParameterType::FLOAT32:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<float>(value)) + ")";
            break;
        case OperatorHandlerParameterType::FLOAT64:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<double>(value)) + ")";
            break;
        case OperatorHandlerParameterType::ENUM_CONSTANT:
            ss << typeName(parameter) + "(" + std::to_string(std::any_cast<int8_t>(value)) + ")";
            break;
        case OperatorHandlerParameterType::SCHEMA: {
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
            ss << ss.str();
            break;
        }
        case OperatorHandlerParameterType::BATCH_JOIN_DEFINITION: {
            auto joinDefinition = any_cast<NES::Join::Experimental::LogicalBatchJoinDefinitionPtr>(value);
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
            break;
        }
        default: NES_THROW_RUNTIME_ERROR("Not implemented for type");
    }
}
void OperatorHandlerCppExporter::HandlerExport::generateHandlerInstantiation(
    size_t index,
    const Runtime::Unikernel::OperatorHandlerDescriptor& handler) {
    ss << "static " << handler.className << " handler" << index << "(";
    if (!handler.parameters.empty()) {
        generateParameter(handler.parameters[0]);
        for (const auto& parameter : handler.parameters | std::ranges::views::drop(1)) {
            ss << ", ";
            generateParameter(parameter);
        }
    }
    ss << ");\n";
}
void OperatorHandlerCppExporter::HandlerExport::generateLocalHandlerFunction(const Stage& stage) {
    ss << "template<>" << std::endl;
    ss << OP_HANDLER << "* NES::Unikernel::Stage<" << stage.pipeline->getPipelineId() << ">::getOperatorHandler(int index) {"
       << std::endl;
    for (const auto& [local, handler] : stage.handler) {
        generateHandlerInstantiation(local, handler);
    }
    ss << "switch(index) {" << std::endl;
    for (const auto& key : stage.handler | std::views::keys) {
        ss << "case " << key << ":\n";
        ss << "\treturn &handler" << key << ";\n";
    }
    for (const auto& [local, shared] : stage.sharedHandlerIndices) {
        ss << "case " << local << ":\n";
        ss << "\treturn getSharedOperatorHandler<" << subPlanId << ">(" << shared << ");";
    }
    ss << "default:\n";
    ss << "\tassert(false && \"Bad OperatorHandler index\");\n";
    ss << "\treturn nullptr;";
    ss << "}";
    ss << "}";
}
void OperatorHandlerCppExporter::HandlerExport::generateSharedHandlerInstantiation(
    const std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>& sharedHandler) {
    ss << "template<>" << std::endl;
    ss << OP_HANDLER << "* getSharedOperatorHandler<" << subPlanId << ">(int index) {" << std::endl;
    for (const auto& [idx, handler] : sharedHandler) {
        generateHandlerInstantiation(idx, handler);
    }
    ss << "switch(index) {" << std::endl;
    for (const auto& idx : sharedHandler | std::views::keys) {
        ss << "case " << idx << ":\n";
        ss << "\treturn &handler" << idx << ";\n";
    }
    ss << "default:\n";
    ss << "\tassert(false && \"Bad SharedOperatorHandler index\");\n";
    ss << "\treturn nullptr;";
    ss << "}";
    ss << "}";
}
void OperatorHandlerCppExporter::HandlerExport::externSharedHandlerDefintion() {
    ss << "template<size_t SubQueryPlanId>\n";
    ss << "extern " << OP_HANDLER << "* getSharedOperatorHandler(int index);\n";
}

void OperatorHandlerCppExporter::HandlerExport::sharedHandlerDefintion() {
    ss << "template<size_t SubQueryPlanId>\n";
    ss << OP_HANDLER << "* getSharedOperatorHandler(int index);\n";
}

std::string OperatorHandlerCppExporter::generateHandler(const Stage& stage, NES::QuerySubPlanId subPlanId) {
    HandlerExport exporter(subPlanId);

    if (!stage.sharedHandlerIndices.empty()) {
        exporter.externSharedHandlerDefintion();
    }
    exporter.generateLocalHandlerFunction(stage);

    return exporter.str();
}
std::string OperatorHandlerCppExporter::generateSharedHandler(
    const std::unordered_map<SharedOperatorHandlerIndex, Runtime::Unikernel::OperatorHandlerDescriptor>& sharedHandler,
    NES::QuerySubPlanId subPlanId) {
    NES_ASSERT(!sharedHandler.empty(), "Should not be called without any shared handlers");

    std::vector<std::string> includes;
    std::ranges::transform(sharedHandler, std::back_inserter(includes), [](const auto& kv) {
        return kv.second.headerPath;
    });

    HandlerExport exporter(subPlanId);
    exporter.runtimeIncludes(includes);
    exporter.sharedHandlerDefintion();
    exporter.generateSharedHandlerInstantiation(sharedHandler);

    return exporter.str();
}
}// namespace NES::Unikernel::Export
