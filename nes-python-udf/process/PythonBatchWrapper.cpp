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
#include <UdfProcessCall.hpp>

namespace NES
{

namespace
{
/// `struct`-style format of a fixed-size column, as memoryview.cast takes it.
std::string_view memoryviewFormat(const DataType& type)
{
    switch (type.type)
    {
        case DataType::Type::INT8:
            return "b";
        case DataType::Type::UINT8:
            return "B";
        case DataType::Type::INT16:
            return "h";
        case DataType::Type::UINT16:
            return "H";
        case DataType::Type::INT32:
            return "i";
        case DataType::Type::UINT32:
            return "I";
        case DataType::Type::INT64:
            return "q";
        case DataType::Type::UINT64:
            return "Q";
        case DataType::Type::FLOAT32:
            return "f";
        case DataType::Type::FLOAT64:
            return "d";
        case DataType::Type::BOOLEAN:
        case DataType::Type::VARSIZED:
        case DataType::Type::CHAR:
        case DataType::Type::UNDEFINED:
            break;
    }
    throw CannotLoadUdf("UDF type {} is no fixed-size column of a UDF process", type);
}

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

std::string pythonConstant(const UdfProcessArgument& argument)
{
    const auto text = pythonString(argument.constant.value());
    if (argument.constantType == "VARSIZED")
    {
        return fmt::format("{}.encode()", text);
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

/// Definitions at the start of the batch function, giving access to a column of `n` rows.
std::string bindColumn(const size_t index, const ArrowBufferRef::Field& field)
{
    const auto offset = field.dataOffset;
    if (field.type.type == DataType::Type::VARSIZED)
    {
        return fmt::format(
            "    o{0} = m[{1}:{1} + 4 * (n + 1)].cast(\"i\")\n    p{0} = payloads[{2}]\n", index, offset, field.childBufferSlot);
    }
    if (field.type.type == DataType::Type::BOOLEAN)
    {
        return "";
    }
    return fmt::format("    c{0} = m[{1}:{1} + {2} * n].cast(\"{3}\")\n", index, offset, field.dataTypeSize, memoryviewFormat(field.type));
}

std::string readValue(const size_t index, const ArrowBufferRef::Field& field)
{
    const auto value = [&]
    {
        switch (field.type.type)
        {
            case DataType::Type::VARSIZED:
                return fmt::format("bytes(p{0}[o{0}[i]:o{0}[i + 1]])", index);
            case DataType::Type::BOOLEAN:
                return fmt::format("bool((buf[{} + (i >> 3)] >> (i & 7)) & 1)", field.dataOffset);
            default:
                return fmt::format("c{}[i]", index);
        }
    }();
    /// The bitmap of a non-nullable column is never written.
    if (field.type.nullable)
    {
        return fmt::format("{} if (buf[{} + (i >> 3)] >> (i & 7)) & 1 else None", value, field.bitmapOffset);
    }
    return value;
}

std::string setBit(const uint64_t region, const bool value)
{
    if (value)
    {
        return fmt::format("buf[{} + (i >> 3)] |= 1 << (i & 7)", region);
    }
    return fmt::format("buf[{} + (i >> 3)] &= ~(1 << (i & 7)) & 255", region);
}

constexpr std::string_view RUNTIME = R"(

def _read_exactly(target):
    view = memoryview(target)
    while view:
        received = sock.recv_into(view)
        if received == 0:
            raise EOFError
        view = view[received:]


def _failure(error):
    message = "".join(traceback.format_exception_only(type(error), error)).strip().encode()
    sock.sendall(struct.pack("<QQ", 0, len(message)) + message)


def _main():
    lengths = struct.Struct("<{0}Q".format(VARSIZED_COLUMNS))
    header = bytearray(8 + lengths.size)
    buf = bytearray(BUFFER_SIZE)
    while True:
        try:
            _read_exactly(header)
        except EOFError:
            return
        n = struct.unpack_from("<Q", header)[0]
        payloads = [bytearray(length) for length in lengths.unpack_from(header, 8)]
        _read_exactly(buf)
        for payload in payloads:
            _read_exactly(payload)
        try:
            _process_batch(buf, payloads, n)
        except Exception as error:
            _failure(error)
            continue
        sock.sendall(struct.pack("<Q", 1) + lengths.pack(*map(len, payloads)))
        sock.sendall(buf)
        for payload in payloads:
            sock.sendall(payload)


try:
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
    const auto varSizedColumns = std::ranges::count_if(
        fields, [](const ArrowBufferRef::Field& field) { return field.childBufferSlot != ArrowBufferRef::Field::NO_CHILD_BUFFER; });

    std::string program = fmt::format(
        "import importlib, os, struct, sys, traceback\n\n"
        "BUFFER_SIZE = {}\n"
        "VARSIZED_COLUMNS = {}\n"
        "sys.path[:0] = [path for path in os.environ.get(\"NES_UDF_PATH\", \"\").split(\":\") if path]\n\n\n"
        "def _define_udfs():\n"
        "    namespace = globals()\n"
        "    exec(_UDF_SOURCE, namespace)\n\n\n",
        layout.getBufferSize(),
        varSizedColumns);

    std::string udfs;
    std::string columns;
    std::string results;
    std::string loop;
    std::string finish;
    std::vector<bool> bound(fields.size(), false);
    const auto bind = [&](const size_t index)
    {
        if (!bound[index])
        {
            columns += bindColumn(index, fields.at(index));
            bound[index] = true;
        }
    };

    for (const auto& [callIndex, call] : std::views::enumerate(calls))
    {
        udfs += defineUdf(callIndex, call);

        std::vector<std::string> arguments;
        for (const auto& [argumentIndex, argument] : std::views::enumerate(call.arguments))
        {
            if (argument.field.has_value())
            {
                bind(*argument.field);
                arguments.push_back(readValue(*argument.field, fields.at(*argument.field)));
            }
            else
            {
                const auto name = fmt::format("_constant_{}_{}", callIndex, argumentIndex);
                udfs += fmt::format("{} = {}\n", name, pythonConstant(argument));
                arguments.push_back(name);
            }
        }

        const auto resultIndex = call.resultField;
        const auto& result = fields.at(resultIndex);
        INVARIANT(result.type.nullable, "The result column of UDF '{}' must be nullable", call.name);
        const auto valid = setBit(result.bitmapOffset, true);
        const auto null = setBit(result.bitmapOffset, false);
        loop += fmt::format("        r = _udf_{}({})\n", callIndex, fmt::join(arguments, ", "));
        if (result.type.type == DataType::Type::VARSIZED)
        {
            results += fmt::format(
                "    w{0} = m[{1}:{1} + 4 * (n + 1)].cast(\"i\")\n    w{0}[0] = 0\n    t{0} = 0\n    j{0} = []\n",
                resultIndex,
                result.dataOffset);
            loop += fmt::format(
                "        if r is None:\n            {1}\n        else:\n            {2}\n"
                "            r = r.encode() if isinstance(r, str) else r\n"
                "            if not isinstance(r, (bytes, bytearray, memoryview)):\n"
                "                raise TypeError(\"a VARSIZED UDF must return bytes or str, not \" + type(r).__name__)\n"
                "            j{0}.append(r)\n            t{0} += len(r)\n"
                "        w{0}[i + 1] = t{0}\n",
                resultIndex,
                null,
                valid);
            finish += fmt::format("    payloads[{}] = b\"\".join(j{})\n", result.childBufferSlot, resultIndex);
        }
        else if (result.type.type == DataType::Type::BOOLEAN)
        {
            loop += fmt::format(
                "        if r is None:\n            {}\n        else:\n            {}\n"
                "            if r:\n                {}\n            else:\n                {}\n",
                null,
                valid,
                setBit(result.dataOffset, true),
                setBit(result.dataOffset, false));
        }
        else
        {
            results += fmt::format(
                "    w{0} = m[{1}:{1} + {2} * n].cast(\"{3}\")\n",
                resultIndex,
                result.dataOffset,
                result.dataTypeSize,
                memoryviewFormat(result.type));
            loop += fmt::format(
                "        if r is None:\n            {1}\n        else:\n            {2}\n            w{0}[i] = r\n",
                resultIndex,
                null,
                valid);
        }
    }

    program += fmt::format("_UDF_SOURCE = {}\n\n\n", pythonString(udfs));
    program += fmt::format(
        "def _process_batch(buf, payloads, n):\n    m = memoryview(buf)\n{}{}    for i in range(n):\n{}{}", columns, results, loop, finish);
    program += RUNTIME;
    return program;
}

}
