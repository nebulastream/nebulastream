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
#include <Functions/PythonPhysicalFunction.hpp>
#include <Functions/UDFCallLogicalFunction.hpp>
#include <Functions/UDFPhysicalFunction.hpp>
#include <Schema/Binder.hpp>
#include <Traits/FieldMappingTrait.hpp>
#include <Util/Strings.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>
#include <PhysicalFunctionRegistry.hpp>
#include <UdfBackend.hpp>
#include <UdfBridgeRegistry.hpp>
#include <UdfDescriptor.hpp>

namespace NES::QueryCompilation
{
namespace
{
/// Wraps an inline PYTHON(...) body as a plain, top-level Python function so it can be handed to
/// initialize_udf_from_source: the CPython/PyPy ABI marshals arguments as ordinary Python values
/// already, so no per-argument unwrapping is needed here (unlike the Codon path, which must also
/// generate its own C-ABI trampoline).
std::string buildInlinePythonSource(const std::vector<std::string>& parameterNames, const std::string& body, std::string_view functionName)
{
    return fmt::format("def {}({}):\n{}\n", functionName, fmt::join(parameterNames, ", "), body);
}

/// A catalog UDF that executes through Codon (CREATE FUNCTION ... BRIDGE 'codon'), expressed as the inline function
/// the Codon path already compiles: the ENTRYPOINT module is imported and called from a generated body.
struct CodonUdfCall
{
    std::vector<std::string> parameterNames;
    std::string body;
};

/// Reproduces the strict-UDF semantics of the interpreter bridges (a NULL argument yields NULL without calling the UDF)
/// in the generated body, so the imported function only ever sees plain values, never Optional[T].
CodonUdfCall buildCodonUdfCall(const UdfDescriptor& descriptor, const std::vector<DataType>& inputTypes)
{
    const auto entrypoint = splitEntrypoint(descriptor.getEntrypoint());
    INVARIANT(entrypoint.has_value(), "Codon UDF '{}' has no 'module.function' entry point", descriptor.getName());
    INVARIANT(
        inputTypes.size() == descriptor.getArgTypes().size(), "UDF '{}' called with a wrong number of arguments", descriptor.getName());

    CodonUdfCall call;
    std::vector<std::string> arguments;
    call.body = fmt::format("from {} import {} as _nes_udf_entry\n", entrypoint->first, entrypoint->second);
    for (size_t index = 0; index < inputTypes.size(); ++index)
    {
        call.parameterNames.emplace_back(fmt::format("_p{}", index));
        if (inputTypes[index].nullable)
        {
            call.body += fmt::format("if _p{} is None:\n    return None\n", index);
            arguments.emplace_back(fmt::format("_p{}.__val__()", index));
        }
        else
        {
            arguments.emplace_back(fmt::format("_p{}", index));
        }
    }
    call.body += fmt::format("return _nes_udf_entry({})", fmt::join(arguments, ", "));
    return call;
}

/// Where Codon looks for the ENTRYPOINT module: the configured python_udf_import_paths plus NES_UDF_PATH, the module
/// search path the CPython and PyPy bridges use, so a single directory of UDF modules serves every backend.
std::vector<std::string> codonImportPaths(const std::vector<std::string>& configured)
{
    auto paths = configured;
    const auto* const udfPath = std::getenv("NES_UDF_PATH"); /// NOLINT(concurrency-mt-unsafe)
    if (udfPath == nullptr)
    {
        return paths;
    }
    const std::string_view remaining{udfPath};
    size_t begin = 0;
    while (begin <= remaining.size())
    {
        const auto end = remaining.find(':', begin);
        const auto directory = std::string{remaining.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin)};
        if (!directory.empty() && std::ranges::find(paths, directory) == paths.end())
        {
            paths.push_back(directory);
        }
        if (end == std::string_view::npos)
        {
            break;
        }
        begin = end + 1;
    }
    return paths;
}

/// Bridge name resolveBuiltinUdfBridgePath expects; Codon never reaches here (handled separately).
std::string_view interpreterBridgeName(const PythonUdfBackend backend)
{
    switch (backend)
    {
        case PythonUdfBackend::CPython:
            return "cpython";
        case PythonUdfBackend::PyPy:
            return "pypy";
        case PythonUdfBackend::Codon:
            break;
    }
    std::unreachable();
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
        if (descriptor->getExecution() == UdfExecution::Codon)
        {
            INVARIANT(logicalFunction.getDataType().nullable, "UDF call results are nullable");
            auto codonCall = buildCodonUdfCall(*descriptor, inputTypes);
            return PhysicalFunction{PythonPhysicalFunction(
                                        std::move(codonCall.parameterNames),
                                        std::move(codonCall.body),
                                        childFunctions,
                                        inputTypes,
                                        logicalFunction.getDataType(),
                                        codonImportPaths(pythonUdfImportPaths))}
                .withSetupChildren(std::move(childFunctions));
        }
        return PhysicalFunction{UDFPhysicalFunction(
                                    childFunctions, descriptor->getArgTypes(), logicalFunction.getDataType(), UdfBackend::create(*descriptor))}
            .withSetupChildren(std::move(childFunctions));
    }
    /// Inline Python UDFs (PYTHON((...): $python$ ... $python$)) carry their source directly in the query
    /// text rather than a catalog descriptor, so they bypass the registry the same way. BRIDGE selects
    /// how: Codon (default) AOT-compiles the body into the pipeline itself; cpython/pypy instead run it
    /// through the same interpreter bridges file-based UDFs use, with no catalog entry.
    if (const auto pythonFunction = logicalFunction.tryGetAs<PythonLogicalFunction>())
    {
        if (pythonFunction.value()->getBackend() == PythonUdfBackend::Codon)
        {
            return PhysicalFunction{PythonPhysicalFunction(
                                        pythonFunction.value()->getParameterNames(),
                                        pythonFunction.value()->getBody(),
                                        childFunctions,
                                        inputTypes,
                                        logicalFunction.getDataType(),
                                        pythonUdfImportPaths)}
                .withSetupChildren(std::move(childFunctions));
        }
        static constexpr std::string_view InlineFunctionName = "nes_inline_udf";
        const auto bridgePath = resolveBuiltinUdfBridgePath(interpreterBridgeName(pythonFunction.value()->getBackend()));
        const auto source = buildInlinePythonSource(pythonFunction.value()->getParameterNames(), pythonFunction.value()->getBody(), InlineFunctionName);
        return PhysicalFunction{UDFPhysicalFunction(
                                    childFunctions,
                                    inputTypes,
                                    logicalFunction.getDataType(),
                                    UdfBackend::createFromSource(
                                        bridgePath, source, std::string(InlineFunctionName), inputTypes, logicalFunction.getDataType()))}
            .withSetupChildren(std::move(childFunctions));
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
