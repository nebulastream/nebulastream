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

/// Runs an INLINE Python UDF without the query engine: no source, no CSV parsing per query, no pipeline and no sink.
///
/// The UDF is compiled as CREATE FUNCTION compiles it (UdfCatalog, with the Codon or Numba compiler), and the interpreter
/// entry point of the compiled bitcode is JIT-compiled with LLVM ORC at -O3. The JIT resolves the same runtime functions as
/// the worker: seq_alloc on the scratch region of the call, the Codon plugin natives, and for Numba the NES Numba runtime
/// with plain reference counts. Each call resets the scratch region first, as the worker does.
///
/// The runner reads the argument columns of a delimited file into memory once, then calls the UDF on each row in a loop.
/// Round 0 also runs the module initializer of a Codon UDF at its first call and writes the results (--output); the later
/// rounds are timed. It prints one JSON line.
///
/// Usage:
///   nes-python-udf-runner --compiler Codon|Numba --entrypoint module.function --import-path DIR --returns TYPE
///       --arg column:INDEX:TYPE | --arg literal:TYPE:VALUE  (once per UDF argument, in order)
///       --input FILE [--delimiter TAB] [--rows N] [--rounds R] [--output FILE] [--optimize 0|1]
/// Types are NES type names (VARSIZED, INT32, INT64, FLOAT64, ...). An empty field of a column is NULL. Numba needs
/// NES_NUMBA_PYTHON, as in the worker.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iostream>
#include <memory>
#include <optional>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <NESCodonPlugin/Registry.hpp>
#include <NumbaRuntime.hpp>
#include <Plugins/BuiltinPlugins.hpp>
#include <PythonUdfCompiler.hpp>
#include <PythonUdfRuntime.hpp>
#include <UdfCatalog.hpp>
#include <UdfDescriptor.hpp>
#include <fmt/format.h>
#include <llvm/Bitcode/BitcodeReader.h>
#include <llvm/ExecutionEngine/Orc/ExecutionUtils.h>
#include <llvm/ExecutionEngine/Orc/LLJIT.h>
#include <llvm/ExecutionEngine/Orc/ThreadSafeModule.h>
#include <llvm/IR/LLVMContext.h>
#include <llvm/IR/Module.h>
#include <llvm/Passes/PassBuilder.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/MemoryBuffer.h>
#include <llvm/Support/TargetSelect.h>
#include <magic_enum/magic_enum.hpp>

namespace
{
using NES::DataType;
using NES::PythonUdfAbiValue;
using NES::UdfErrorHolder;
using EntryPoint = uint8_t (*)(PythonUdfAbiValue*, PythonUdfAbiValue*, UdfErrorHolder*);

struct Options
{
    std::string compiler = "Codon";
    std::string entrypoint;
    std::vector<std::string> importPaths;
    std::string returns;
    std::vector<std::string> arguments;
    std::string input;
    char delimiter = '\t';
    size_t rows = SIZE_MAX;
    size_t rounds = 3;
    std::string output;
    bool optimize = true;
};

Options parseOptions(const int argc, char** argv)
{
    Options options;
    for (int index = 1; index < argc; ++index)
    {
        const std::string_view name = argv[index];
        if (index + 1 >= argc)
        {
            throw std::invalid_argument(fmt::format("{} needs a value", name));
        }
        const std::string value = argv[++index];
        if (name == "--compiler")
        {
            options.compiler = value;
        }
        else if (name == "--entrypoint")
        {
            options.entrypoint = value;
        }
        else if (name == "--import-path")
        {
            options.importPaths.push_back(value);
        }
        else if (name == "--returns")
        {
            options.returns = value;
        }
        else if (name == "--arg")
        {
            options.arguments.push_back(value);
        }
        else if (name == "--input")
        {
            options.input = value;
        }
        else if (name == "--delimiter")
        {
            options.delimiter = value == "TAB" ? '\t' : value.at(0);
        }
        else if (name == "--rows")
        {
            options.rows = std::stoull(value);
        }
        else if (name == "--rounds")
        {
            options.rounds = std::stoull(value);
        }
        else if (name == "--optimize")
        {
            options.optimize = value != "0";
        }
        else if (name == "--output")
        {
            options.output = value;
        }
        else
        {
            throw std::invalid_argument(fmt::format("unknown option {}", name));
        }
    }
    if (options.entrypoint.empty() || options.returns.empty() || options.arguments.empty() || options.input.empty())
    {
        throw std::invalid_argument("--entrypoint, --returns, --arg and --input are required");
    }
    return options;
}

std::vector<std::string> split(const std::string_view text, const char delimiter)
{
    std::vector<std::string> parts;
    size_t begin = 0;
    while (true)
    {
        const auto end = text.find(delimiter, begin);
        parts.emplace_back(text.substr(begin, end == std::string_view::npos ? std::string_view::npos : end - begin));
        if (end == std::string_view::npos)
        {
            return parts;
        }
        begin = end + 1;
    }
}

/// One argument of the UDF for every row: the text of a column or a literal, and for a fixed-size type its value in 8 bytes.
struct Argument
{
    DataType type;
    std::vector<std::string> texts;
    std::vector<uint64_t> scalars;
    /// An empty field of a column is NULL, as for the nullable columns of the UDFBench sources.
    std::vector<uint8_t> nulls;
    bool literal = false;
};

uint64_t scalarBits(const DataType& type, const std::string& text)
{
    uint64_t bits = 0;
    const auto store = [&](const auto value) { std::memcpy(&bits, &value, sizeof(value)); };
    switch (type.type)
    {
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
            store(static_cast<int64_t>(std::stoll(text)));
            break;
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
            store(static_cast<uint64_t>(std::stoull(text)));
            break;
        case DataType::Type::FLOAT32:
            store(std::stof(text));
            break;
        case DataType::Type::FLOAT64:
            store(std::stod(text));
            break;
        case DataType::Type::BOOLEAN:
            store(static_cast<uint8_t>(text == "true" || text == "1" ? 1 : 0));
            break;
        default:
            throw std::invalid_argument(fmt::format("argument type {} is not supported", magic_enum::enum_name(type.type)));
    }
    return bits;
}

DataType dataType(const std::string& name, const DataType::NULLABLE nullable)
{
    const auto type = magic_enum::enum_cast<DataType::Type>(name);
    if (!type || *type == DataType::Type::UNDEFINED)
    {
        throw std::invalid_argument(fmt::format("unknown type {}", name));
    }
    return DataType{*type, nullable};
}

size_t scalarSize(const DataType& type)
{
    return type.getSizeInBytesWithoutNull();
}

std::vector<Argument> loadArguments(const Options& options, size_t& rowCount)
{
    std::vector<std::pair<size_t, size_t>> columns; /// (argument, column)
    std::vector<Argument> arguments;
    for (const auto& spec : options.arguments)
    {
        const auto parts = split(spec, ':');
        if (parts.size() < 3 || (parts[0] != "column" && parts[0] != "literal"))
        {
            throw std::invalid_argument(fmt::format("--arg {}: expected column:INDEX:TYPE or literal:TYPE:VALUE", spec));
        }
        Argument argument;
        if (parts[0] == "column")
        {
            argument.type = dataType(parts[2], DataType::NULLABLE::NOT_NULLABLE);
            columns.emplace_back(arguments.size(), std::stoull(parts[1]));
        }
        else
        {
            argument.type = dataType(parts[1], DataType::NULLABLE::NOT_NULLABLE);
            argument.literal = true;
            const auto value = spec.substr(parts[0].size() + parts[1].size() + 2);
            argument.texts.push_back(value);
            if (argument.type.type != DataType::Type::VARSIZED)
            {
                argument.scalars.push_back(scalarBits(argument.type, value));
            }
        }
        arguments.push_back(std::move(argument));
    }

    std::ifstream file(options.input, std::ios::binary);
    if (!file)
    {
        throw std::runtime_error(fmt::format("cannot read {}", options.input));
    }
    rowCount = 0;
    std::string line;
    while (rowCount < options.rows && std::getline(file, line))
    {
        const auto fields = split(line, options.delimiter);
        for (const auto& [argumentIndex, column] : columns)
        {
            auto& argument = arguments[argumentIndex];
            const auto& field = fields.at(column);
            argument.nulls.push_back(field.empty() ? 1 : 0);
            argument.texts.push_back(argument.type.type == DataType::Type::VARSIZED ? field : std::string{});
            argument.scalars.push_back(field.empty() || argument.type.type == DataType::Type::VARSIZED ? 0 : scalarBits(argument.type, field));
        }
        ++rowCount;
    }
    return arguments;
}

NES::CompiledInlineUdf compile(const Options& options, const std::vector<Argument>& arguments)
{
    std::vector<DataType> argumentTypes;
    for (const auto& argument : arguments)
    {
        argumentTypes.push_back(argument.type);
    }
    NES::UdfCatalog catalog{options.importPaths};
    catalog.registerUdf(
        "runner_udf",
        "",
        options.entrypoint,
        argumentTypes,
        dataType(options.returns, DataType::NULLABLE::IS_NULLABLE),
        NES::UdfExecution::Inline,
        std::nullopt,
        NES::UdfLanguage::Python,
        {},
        options.compiler);
    const auto compiled = catalog.load("runner_udf").getCompiled();
    if (!compiled)
    {
        throw std::runtime_error("the catalog did not compile the UDF");
    }
    return *compiled;
}

template <typename T>
T check(llvm::Expected<T> value, const std::string_view what)
{
    if (!value)
    {
        throw std::runtime_error(fmt::format("{}: {}", what, llvm::toString(value.takeError())));
    }
    return std::move(*value);
}

void check(llvm::Error error, const std::string_view what)
{
    if (error)
    {
        throw std::runtime_error(fmt::format("{}: {}", what, llvm::toString(std::move(error))));
    }
}

/// Keeps the JIT alive for as long as the entry point is used.
struct JittedUdf
{
    std::unique_ptr<llvm::orc::LLJIT> jit;
    EntryPoint entry;
};

JittedUdf jit(const NES::CompiledInlineUdf& compiled, const bool numba, const bool optimize)
{
    llvm::InitializeNativeTarget();
    llvm::InitializeNativeTargetAsmPrinter();
    auto jit = check(llvm::orc::LLJITBuilder().create(), "creating the JIT");

    /// The runtime functions of the worker (PythonPhysicalFunction.cpp, runtimeSymbols).
    llvm::orc::SymbolMap symbols;
    const auto add = [&](const std::string_view name, void* address)
    {
        symbols[jit->mangleAndIntern(llvm::StringRef{name.data(), name.size()})]
            = {llvm::orc::ExecutorAddr::fromPtr(address), llvm::JITSymbolFlags::Exported | llvm::JITSymbolFlags::Callable};
    };
    for (const auto& [name, address] : NES::getPythonUdfRuntimeSymbols())
    {
        add(name, address);
    }
    if (numba)
    {
        for (const auto& [name, address] : NES::getNumbaRuntimeSymbols())
        {
            add(name, address);
        }
        NES::useArenaForNumbaRuntime();
    }
    else
    {
        for (const auto& [name, address] : NES::getCodonPluginNativeSymbols())
        {
            add(name, address);
        }
    }
    auto& library = jit->getMainJITDylib();
    check(library.define(llvm::orc::absoluteSymbols(std::move(symbols))), "defining the runtime symbols");
    /// What LLVM itself emits calls to, e.g. memcpy and memset.
    library.addGenerator(check(
        llvm::orc::DynamicLibrarySearchGenerator::GetForCurrentProcess(jit->getDataLayout().getGlobalPrefix()), "process symbols"));

    auto context = std::make_unique<llvm::LLVMContext>();
    const auto& bitcode = compiled.interpreterBitcode;
    auto module = check(
        llvm::parseBitcodeFile(llvm::MemoryBufferRef{llvm::StringRef{bitcode.data(), bitcode.size()}, "udf"}, *context),
        "reading the bitcode");

    /// -O3 over the UDF module, as the query compiler optimizes a pipeline with the UDF linked in (--optimize 0 skips it).
    llvm::LoopAnalysisManager loops;
    llvm::FunctionAnalysisManager functions;
    llvm::CGSCCAnalysisManager cgscc;
    llvm::ModuleAnalysisManager modules;
    llvm::PassBuilder passes;
    passes.registerModuleAnalyses(modules);
    passes.registerCGSCCAnalyses(cgscc);
    passes.registerFunctionAnalyses(functions);
    passes.registerLoopAnalyses(loops);
    passes.crossRegisterProxies(loops, functions, cgscc, modules);
    if (optimize)
    {
        passes.buildPerModuleDefaultPipeline(llvm::OptimizationLevel::O3).run(*module, modules);
    }

    check(jit->addIRModule(llvm::orc::ThreadSafeModule(std::move(module), std::move(context))), "adding the UDF");
    const auto address = check(jit->lookup(NES::interpreterSymbolName(compiled.symbol)), "finding the entry point");
    return {std::move(jit), address.toPtr<EntryPoint>()};
}

std::string formatResult(const DataType& type, const PythonUdfAbiValue& result, const uint64_t storage)
{
    if (result.isNull != 0)
    {
        return "NULL";
    }
    const auto load = [&]<typename T>(T) -> T
    {
        T value{};
        std::memcpy(&value, &storage, sizeof(T));
        return value;
    };
    switch (type.type)
    {
        case DataType::Type::VARSIZED:
            return {reinterpret_cast<const char*>(result.data), result.size};
        case DataType::Type::INT8:
            return std::to_string(load(int8_t{}));
        case DataType::Type::INT16:
            return std::to_string(load(int16_t{}));
        case DataType::Type::INT32:
            return std::to_string(load(int32_t{}));
        case DataType::Type::INT64:
            return std::to_string(load(int64_t{}));
        case DataType::Type::UINT8:
        case DataType::Type::BOOLEAN:
            return std::to_string(load(uint8_t{}));
        case DataType::Type::UINT16:
            return std::to_string(load(uint16_t{}));
        case DataType::Type::UINT32:
            return std::to_string(load(uint32_t{}));
        case DataType::Type::UINT64:
            return std::to_string(load(uint64_t{}));
        case DataType::Type::FLOAT32:
            return fmt::format("{}", load(float{}));
        case DataType::Type::FLOAT64:
            return fmt::format("{}", load(double{}));
        default:
            return "?";
    }
}

int run(const Options& options)
{
    size_t rowCount = 0;
    const auto loadStart = std::chrono::steady_clock::now();
    auto arguments = loadArguments(options, rowCount);
    const auto compileStart = std::chrono::steady_clock::now();
    const auto compiled = compile(options, arguments);
    const auto jitStart = std::chrono::steady_clock::now();
    const auto udf = jit(compiled, options.compiler == "Numba" || options.compiler == "NUMBA", options.optimize);
    const auto jitEnd = std::chrono::steady_clock::now();
    const auto returnType = dataType(options.returns, DataType::NULLABLE::IS_NULLABLE);

    std::vector<PythonUdfAbiValue> abiArguments(arguments.size());
    /// The 8-byte values of fixed-size arguments for the current row; the ABI passes them by pointer.
    std::vector<uint64_t> argumentStorage(arguments.size());
    std::ofstream output;
    if (!options.output.empty())
    {
        output.open(options.output, std::ios::binary);
    }

    std::vector<double> roundSeconds;
    size_t errors = 0;
    uint64_t checksum = 0;
    for (size_t round = 0; round <= options.rounds; ++round)
    {
        const auto started = std::chrono::steady_clock::now();
        for (size_t row = 0; row < rowCount; ++row)
        {
            for (size_t index = 0; index < arguments.size(); ++index)
            {
                const auto& argument = arguments[index];
                const auto source = argument.literal ? 0 : row;
                auto& abi = abiArguments[index];
                abi.isNull = argument.literal ? 0 : argument.nulls[row];
                if (argument.type.type == DataType::Type::VARSIZED)
                {
                    const auto& text = argument.texts[source];
                    abi.data = reinterpret_cast<int8_t*>(const_cast<char*>(text.data()));
                    abi.size = text.size();
                }
                else
                {
                    argumentStorage[index] = argument.scalars[source];
                    abi.data = reinterpret_cast<int8_t*>(&argumentStorage[index]);
                    abi.size = scalarSize(argument.type);
                }
            }
            uint64_t resultStorage = 0;
            PythonUdfAbiValue result{.data = reinterpret_cast<int8_t*>(&resultStorage), .size = sizeof(resultStorage), .isNull = 0};
            UdfErrorHolder error{};
            NES::activatePythonUdfArena(nullptr);
            const auto status = udf.entry(abiArguments.data(), &result, &error);
            if (status != 0)
            {
                ++errors;
                if (round == 0 && output.is_open())
                {
                    output << "ERROR " << std::string_view{reinterpret_cast<const char*>(error.errorPtr), error.errorSize} << '\n';
                }
                continue;
            }
            checksum += result.isNull != 0 ? 0 : (returnType.type == DataType::Type::VARSIZED ? result.size : resultStorage);
            if (round == 0 && output.is_open())
            {
                output << formatResult(returnType, result, resultStorage) << '\n';
            }
        }
        roundSeconds.push_back(std::chrono::duration<double>(std::chrono::steady_clock::now() - started).count());
    }

    const auto milliseconds = [](const auto from, const auto to) { return std::chrono::duration<double, std::milli>(to - from).count(); };
    std::vector<double> timed(roundSeconds.begin() + 1, roundSeconds.end());
    std::ranges::sort(timed);
    const auto best = timed.empty() ? roundSeconds.front() : timed.front();
    const auto median = timed.empty() ? roundSeconds.front() : timed[timed.size() / 2];
    std::cout << fmt::format(
        R"({{"compiler": "{}", "entrypoint": "{}", "rows": {}, "rounds": {}, "errors_per_round": {}, "load_ms": {:.1f}, )"
        R"("compile_ms": {:.1f}, "jit_ms": {:.1f}, "first_round_ns_per_row": {:.1f}, "best_ns_per_row": {:.1f}, )"
        R"("median_ns_per_row": {:.1f}, "checksum": {}}})",
        options.compiler,
        options.entrypoint,
        rowCount,
        options.rounds,
        errors / (options.rounds + 1),
        milliseconds(loadStart, compileStart),
        milliseconds(compileStart, jitStart),
        milliseconds(jitStart, jitEnd),
        roundSeconds.front() * 1e9 / static_cast<double>(rowCount),
        best * 1e9 / static_cast<double>(rowCount),
        median * 1e9 / static_cast<double>(rowCount),
        checksum)
              << '\n';
    return errors == 0 ? 0 : 2;
}
}

int main(const int argc, char** argv)
{
    try
    {
        NES::loadBuiltinPlugins();
        return run(parseOptions(argc, argv));
    }
    catch (const std::exception& exception)
    {
        std::cerr << "nes-python-udf-runner: " << exception.what() << '\n';
        return 1;
    }
}
