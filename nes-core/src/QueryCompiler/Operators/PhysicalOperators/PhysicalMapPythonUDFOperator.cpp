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
#ifdef NAUTILUS_PYTHON_UDF_ENABLED
#include "Operators/LogicalOperators/PythonUDFLogicalOperator.hpp"
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapPythonUDFOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
PhysicalMapPythonUDFOperator::PhysicalMapPythonUDFOperator(OperatorId id,
                                                       SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       Catalogs::UDF::PythonUDFDescriptorPtr pythonUDFDescriptor)
    : OperatorNode(id), PhysicalUnaryOperator(id, std::move(inputSchema), std::move(outputSchema)), pythonUDFDescriptor(std::move(pythonUDFDescriptor)) {}

PhysicalOperatorPtr PhysicalMapPythonUDFOperator::create(SchemaPtr inputSchema,
                                                       SchemaPtr outputSchema,
                                                       Catalogs::UDF::PythonUdfDescriptorPtr pythonUDFDescriptor) {
    return create(Util::getNextOperatorId(), inputSchema, outputSchema, pythonUDFDescriptor);
}

PhysicalOperatorPtr PhysicalMapPythonUDFOperator::create(OperatorId id,
                                                       const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const Catalogs::UDF::PythonUDFDescriptorPtr& pythonUDFDescriptor) {
    return std::make_shared<PhysicalMapPythonUDFOperator>(id, inputSchema, outputSchema, pythonUDFDescriptor);
}

std::string PhysicalMapPythonUDFOperator::toString() const { return "PhysicalMapPythonUDFOperator"; }

OperatorNodePtr PhysicalMapPythonUDFOperator::copy() { return create(id, inputSchema, outputSchema, pythonUDFDescriptor); }

Catalogs::UDF::PythonUDFDescriptorPtr PhysicalMapPythonUDFOperator::getPythonUDFDescriptor() { return pythonUDFDescriptor; }
}// namespace NES::QueryCompilation::PhysicalOperators
#endif// NAUTILUS_PYTHON_UDF_ENABLED