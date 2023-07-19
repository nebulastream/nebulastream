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
#include <Catalogs/UDF/PythonUDFDescriptor.hpp>
#include <QueryCompiler/Operators/PhysicalOperators/PhysicalMapPythonUDFOperator.hpp>

namespace NES::QueryCompilation::PhysicalOperators {
PhysicalMapPythonUDFOperator::PhysicalMapPythonUDFOperator(OperatorId id,
                                                       const SchemaPtr& inputSchema,
                                                       const SchemaPtr& outputSchema,
                                                       const Catalogs::UDF::PythonUDFDescriptorPtr& pythonUDFDescriptor)
    : OperatorNode(id), PhysicalUDFOperator(id, std::move(inputSchema), std::move(outputSchema), std::move(pythonUDFDescriptor)) {}

std::string PhysicalMapPythonUDFOperator::toString() const { return "PhysicalMapPythonUDFOperator"; }
}// namespace NES::QueryCompilation::PhysicalOperators
#endif// NAUTILUS_PYTHON_UDF_ENABLED