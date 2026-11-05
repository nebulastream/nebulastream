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

#include <PythonBatchWrapper.hpp>

#include <cstddef>
#include <ranges>
#include <string>
#include <string_view>
#include <vector>

#include <DataTypes/DataType.hpp>
#include <Interface/BufferRef/ArrowBufferRef.hpp>
#include <fmt/format.h>
#include <fmt/ranges.h>
#include <ErrorHandling.hpp>
#include <PythonBytesParameters.hpp>
#include <UdfProcessCall.hpp>

namespace NES
{

namespace
{
/// A Python expression for arbitrary text, safe to splice into the generated code.
std::string pythonString(const std::string_view text)
{
    std::string hex;
    hex.reserve(text.size() * 2);
    for (const char character : text)
    {
        hex += fmt::format("{:02x}", static_cast<unsigned char>(character));
    }
    return fmt::format("bytes.fromhex(\"{}\").decode()", hex);
}

std::string pythonConstant(const UdfProcessArgument& argument, const std::string_view asBytes)
{
    const auto text = pythonString(argument.constant.value());
    if (argument.constantType == "VARSIZED" || argument.constantType == "CHAR")
    {
        return fmt::format("{0}.encode() if {1} else {0}", text, asBytes);
    }
    if (argument.constantType == "BOOLEAN")
    {
        return fmt::format("{}.upper() == \"TRUE\"", text);
    }
    if (argument.constantType == "FLOAT32" || argument.constantType == "FLOAT64")
    {
        return fmt::format("float({})", text);
    }
    return fmt::format("int({})", text);
}

std::string defineUdf(const size_t index, const UdfProcessCall& call)
{
    if (call.body.has_value())
    {
        auto definition = fmt::format("def _udf_{}({}):\n", index, fmt::join(call.parameterNames, ", "));
        /// One level deeper than the body's own indentation, whichever that is.
        for (const auto line : std::views::split(std::string_view{*call.body}, '\n'))
        {
            definition += fmt::format("    {}\n", std::string_view{line});
        }
        return definition;
    }
    const auto lastDot = call.entrypoint.rfind('.');
    if (lastDot == std::string::npos)
    {
        throw CannotLoadUdf("UDF '{}' needs ENTRYPOINT 'module.function', got '{}'", call.name, call.entrypoint);
    }
    return fmt::format(
        "_udf_{} = getattr(importlib.import_module({}), {})\n",
        index,
        pythonString(std::string_view{call.entrypoint}.substr(0, lastDot)),
        pythonString(std::string_view{call.entrypoint}.substr(lastDot + 1)));
}

/// The Python values of column `index` of `batch`, as a list. A VARSIZED column becomes bytes or, failing on invalid UTF-8, str.
std::string readColumn(const size_t index, const ArrowBufferRef::Field& field, const std::string_view asBytes)
{
    if (field.type.type == DataType::Type::VARSIZED)
    {
        return fmt::format("_binary_column(batch, {}, {})", index, asBytes);
    }
    if (field.type.type == DataType::Type::CHAR)
    {
        return fmt::format("_char_column(batch, {}, {})", index, asBytes);
    }
    return fmt::format("batch.column({}).to_pylist()", index);
}

/// A Python expression turning the list `r` of results into the Arrow array of column `index`.
std::string resultArray(const size_t index, const ArrowBufferRef::Field& field)
{
    switch (field.type.type)
    {
        case DataType::Type::VARSIZED:
            return "pa.array([None if v is None else _binary(v) for v in r], type=pa.binary())";
        case DataType::Type::CHAR:
            return "pa.array([None if v is None else _char(v) for v in r], type=pa.binary(1))";
        case DataType::Type::BOOLEAN:
            return "pa.array([None if v is None else bool(v) for v in r], type=pa.bool_())";
        default:
            return fmt::format("pa.array(r, type=batch.schema.field({}).type)", index);
    }
}

constexpr std::string_view HELPERS = R"(

def _binary_column(batch, index, as_bytes):
    column = batch.column(index)
    # Casting to string validates UTF-8.
    return column.to_pylist() if as_bytes else column.cast(pa.string()).to_pylist()


def _char_column(batch, index, as_bytes):
    # Decoding a single byte as UTF-8 fails for anything but ASCII.
    values = batch.column(index).to_pylist()
    return values if as_bytes else [None if v is None else v.decode() for v in values]


def _char(value):
    value = _binary(value)
    if len(value) != 1:
        raise ValueError("a CHAR UDF must return one byte, not " + str(len(value)))
    return value


def _binary(value):
    if isinstance(value, str):
        return value.encode()
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    raise TypeError("a VARSIZED UDF must return bytes or str, not " + type(value).__name__)


)";

constexpr std::string_view RUNTIME = R"(

def _failure(error):
    message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
    sock.sendall(struct.pack("<QQ", 0, len(message)) + message)


def _main():
    reader = pa.ipc.open_stream(sock.makefile("rb"))
    output = sock.makefile("wb")
    writer = None
    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            return
        try:
            result = _process_batch(batch)
        except Exception as error:
            _failure(error)
            continue
        sock.sendall(struct.pack("<Q", 1))
        if writer is None:
            writer = pa.ipc.new_stream(output, result.schema)
        writer.write_batch(result)
        output.flush()


try:
    import pyarrow as pa
    _define_udfs()
except BaseException as error:
    _failure(error)
    raise SystemExit(1)
sock.sendall(struct.pack("<Q", 1))
_main()
)";
}

std::string generatePythonBatchWrapper(const ArrowBufferRef& layout, const std::vector<UdfProcessCall>& calls)
{
    const auto& fields = layout.getFields();

    std::string program = "import importlib, os, struct, sys, traceback\n\n"
                          "sys.path[:0] = [path for path in os.environ.get(\"NES_UDF_PATH\", \"\").split(\":\") if path]\n\n\n"
                          "def _define_udfs():\n"
                          "    namespace = globals()\n"
                          "    exec(_UDF_SOURCE, namespace)\n\n\n";
    program += PythonBytesParametersSource;
    program += HELPERS;

    std::string udfs;
    std::string body = "def _process_batch(batch):\n    n = batch.num_rows\n    columns = batch.columns\n";
    for (const auto& [callIndex, call] : std::views::enumerate(calls))
    {
        udfs += defineUdf(callIndex, call);
        udfs += fmt::format("_udf_{0}_bytes = _bytes_parameters(_udf_{0}, {1})\n", callIndex, call.arguments.size());

        std::vector<std::string> arguments;
        for (const auto& [argumentIndex, argument] : std::views::enumerate(call.arguments))
        {
            const auto asBytes = fmt::format("_udf_{}_bytes[{}]", callIndex, argumentIndex);
            if (argument.field.has_value())
            {
                arguments.push_back(readColumn(*argument.field, fields.at(*argument.field), asBytes));
            }
            else
            {
                const auto name = fmt::format("_constant_{}_{}", callIndex, argumentIndex);
                udfs += fmt::format("{} = {}\n", name, pythonConstant(argument, asBytes));
                arguments.push_back(fmt::format("[{}] * n", name));
            }
        }

        const auto& result = fields.at(call.resultField);
        INVARIANT(result.type.nullable, "The result column of UDF '{}' must be nullable", call.name);
        if (arguments.empty())
        {
            body += fmt::format("    r = [_udf_{}() for _ in range(n)]\n", callIndex);
        }
        else
        {
            body += fmt::format("    r = [_udf_{}(*row) for row in zip({})]\n", callIndex, fmt::join(arguments, ", "));
        }
        body += fmt::format("    columns[{}] = {}\n", call.resultField, resultArray(call.resultField, result));
    }
    body += "    return pa.RecordBatch.from_arrays(columns, schema=batch.schema)\n";

    program += fmt::format("_UDF_SOURCE = {}\n\n\n", pythonString(udfs));
    program += body;
    program += RUNTIME;
    return program;
}

}
