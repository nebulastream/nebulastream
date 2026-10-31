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
#include <Functions/FunctionProvider.hpp>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Functions/CastToTypeLogicalFunction.hpp>
#include <Functions/ConstantValueLogicalFunction.hpp>
#include <Functions/ConstantValuePhysicalFunction.hpp>
#include <Functions/ConstantValueVariableSizePhysicalFunction.hpp>
#include <Functions/FieldAccessLogicalFunction.hpp>
#include <Functions/FieldAccessPhysicalFunction.hpp>
#include <Functions/LogicalFunction.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Functions/PythonLogicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Schema/Binder.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <UdfDescriptor.hpp>
#include <UdfLoweringRegistry.hpp>

namespace NES::QueryCompilation
{
namespace
{
/// The UdfLoweringRegistry key of the backend that executes a UDF.
std::string_view udfBackendKey(const UdfExecution execution)
{
    switch (execution)
    {
        case UdfExecution::InProcess:
            return "InProcess";
        case UdfExecution::Codon:
            return "Codon";
    }
    std::unreachable();
}

std::string_view udfBackendKey(const PythonUdfBackend backend)
{
    switch (backend)
    {
        case PythonUdfBackend::Codon:
            return "Codon";
        case PythonUdfBackend::CPython:
            return "CPython";
        case PythonUdfBackend::PyPy:
            return "PyPy";
    }
    std::unreachable();
}

PhysicalFunction lowerUdf(std::string_view backend, std::string_view udfName, UdfLoweringRegistryArguments arguments)
{
    const auto lower = UdfLoweringRegistry::instance().find(std::string{backend});
    if (!lower)
    {
        throw PythonUdfsNotEnabled(
            "Cannot execute {}: its {} backend is not part of this build, which has no support for Python UDFs (configure with "
            "-DNES_ENABLE_PYTHON_UDFS=ON)",
            udfName,
            backend);
    }
    return (*lower)(std::move(arguments));
}
}

PhysicalFunction FunctionProvider::lowerFunction(
    LogicalFunction logicalFunction, const FieldMappingTrait& fieldMappingTrait, const std::vector<std::string>& pythonUdfImportPaths)
{
    /// 1. Recursively lower the children of the function node.
    std::vector<PhysicalFunction> childFunctions;
    std::vector<DataType> inputTypes;
    for (const auto& child : logicalFunction.getChildren())
    {
        childFunctions.emplace_back(lowerFunction(child, fieldMappingTrait, pythonUdfImportPaths));
        inputTypes.emplace_back(child.getDataType());
    }

    /// 2. The field access and constant value nodes are special as they require a different treatment,
    /// due to them not simply getting a childFunction as a parameter.
    if (const auto fieldAccessFunction = logicalFunction.tryGetAs<FieldAccessLogicalFunction>())
    {
        const auto mappedName = fieldMappingTrait.getMapping(unbind(fieldAccessFunction.value()->getField()));
        INVARIANT(mappedName.has_value(), "Can not find mapping for {}", fieldAccessFunction.value()->getField());
        return FieldAccessPhysicalFunction(mappedName.value());
    }
    if (const auto constantValueFunction = logicalFunction.tryGetAs<ConstantValueLogicalFunction>())
    {
        return lowerConstantFunction(constantValueFunction->get());
    }
    /// UDF calls also bypass the registry: they carry the resolved catalog descriptor (path, entry
    /// point, signature) that the registry arguments cannot express, so we build the physical function
    /// and load the backend directly here — the same treatment as FieldAccess/ConstantValue above.
    if (const auto udfCallFunction = logicalFunction.tryGetAs<UDFCallLogicalFunction>())
    {
        const auto& descriptor = udfCallFunction.value()->getDescriptor();
        INVARIANT(descriptor.has_value(), "UDF '{}' must be resolved before lowering", udfCallFunction.value()->getUdfName());
        return lowerUdf(
            udfBackendKey(descriptor->getExecution()),
            fmt::format("UDF '{}'", udfCallFunction.value()->getUdfName()),
            UdfLoweringRegistryArguments{
                .udf = *descriptor,
                .childFunctions = std::move(childFunctions),
                .inputTypes = std::move(inputTypes),
                .outputType = logicalFunction.getDataType(),
                .importPaths = pythonUdfImportPaths});
    }
    /// Inline UDFs (PYTHON((...): $python$ ... $python$)) carry their source directly in the query text rather than a
    /// catalog descriptor. BRIDGE selects the backend: Codon (default) AOT-compiles the body into the pipeline itself,
    /// cpython/pypy run it through the interpreter bridges catalog UDFs use.
    if (const auto pythonFunction = logicalFunction.tryGetAs<PythonLogicalFunction>())
    {
        return lowerUdf(
            udfBackendKey(pythonFunction.value()->getBackend()),
            "an inline PYTHON(...) function",
            UdfLoweringRegistryArguments{
                .udf
                = InlineUdfSource{.parameterNames = pythonFunction.value()->getParameterNames(), .body = pythonFunction.value()->getBody()},
                .childFunctions = std::move(childFunctions),
                .inputTypes = std::move(inputTypes),
                .outputType = logicalFunction.getDataType(),
                .importPaths = pythonUdfImportPaths});
    }

    /// 3. Calling the registry to create an executable function.
    if (const auto factory = PhysicalFunctionRegistry::instance().find(std::string(logicalFunction.getType())))
    {
        return (*factory)(PhysicalFunctionRegistryArguments{
                              .childFunctions = childFunctions, .inputTypes = inputTypes, .outputType = logicalFunction.getDataType()})
            .withSetupChildren(std::move(childFunctions));
    }
    throw UnknownFunctionType("Can not lower function: {}", logicalFunction);
}

namespace
{
template <typename T>
requires requires(std::string_view input) { from_chars<T>(input); }
T parseConstantValue(std::string_view input)
{
    if (auto value = from_chars<T>(input))
    {
        return *value;
    }
    throw QueryCompilerError("Can not parse constant value \"{}\" into {}", input, NAMEOF_TYPE(T));
}
}

PhysicalFunction FunctionProvider::lowerConstantFunction(const ConstantValueLogicalFunction& constantFunction)
{
    const auto stringValue = constantFunction.getConstantValue();
    switch (constantFunction.getDataType().type)
    {
        case DataType::Type::UINT8:
            return ConstantUInt8ValueFunction(parseConstantValue<uint8_t>(stringValue));
        case DataType::Type::UINT16:
            return ConstantUInt16ValueFunction(parseConstantValue<uint16_t>(stringValue));
        case DataType::Type::UINT32:
            return ConstantUInt32ValueFunction(parseConstantValue<uint32_t>(stringValue));
        case DataType::Type::UINT64:
            return ConstantUInt64ValueFunction(parseConstantValue<uint64_t>(stringValue));
        case DataType::Type::INT8:
            return ConstantInt8ValueFunction(parseConstantValue<int8_t>(stringValue));
        case DataType::Type::INT16:
            return ConstantInt16ValueFunction(parseConstantValue<int16_t>(stringValue));
        case DataType::Type::INT32:
            return ConstantInt32ValueFunction(parseConstantValue<int32_t>(stringValue));
        case DataType::Type::INT64:
            return ConstantInt64ValueFunction(parseConstantValue<int64_t>(stringValue));
        case DataType::Type::FLOAT32:
            return ConstantFloatValueFunction(parseConstantValue<float>(stringValue));
        case DataType::Type::FLOAT64:
            return ConstantDoubleValueFunction(parseConstantValue<double>(stringValue));
        case DataType::Type::BOOLEAN:
            return ConstantBooleanValueFunction(parseConstantValue<bool>(stringValue));
        case DataType::Type::CHAR:
            return ConstantCharValueFunction(parseConstantValue<char>(stringValue));
        case DataType::Type::VARSIZED: {
            return ConstantValueVariableSizePhysicalFunction(std::bit_cast<const int8_t*>(stringValue.c_str()), stringValue.size());
        };
        case DataType::Type::UNDEFINED: {
            throw UnknownPhysicalType("the UNKNOWN type is not supported");
        };
    }
    std::unreachable();
}
}
