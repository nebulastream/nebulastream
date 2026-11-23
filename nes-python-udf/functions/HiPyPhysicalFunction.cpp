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

#include <HiPyPhysicalFunction.hpp>

#include <algorithm>
#include <charconv>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>
#include <DataTypes/DataType.hpp>
#include <DataTypes/VarVal.hpp>
#include <DataTypes/VariableSizedData.hpp>
#include <Functions/PhysicalFunction.hpp>
#include <Interface/Record.hpp>
#include <fmt/format.h>
#include <nautilus/function.hpp>
#include <nautilus/select.hpp>
#include <nautilus/static.hpp>
#include <nautilus/std/cstring.h>
#include <nautilus/val.hpp>
#include <nautilus/val_ptr.hpp>
#include <nlohmann/json.hpp>
#include <Arena.hpp>
#include <ErrorHandling.hpp>
#include <UdfDescriptor.hpp>
#include <val_arith.hpp>
#include <val_bool.hpp>

/// NOLINTBEGIN(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,cppcoreguidelines-pro-type-reinterpret-cast)
namespace NES
{
namespace HiPy
{

/// ---------------------------------------------------------------------------------------------------------------------------
/// The IR, parsed once from the JSON of nes_hipy_compile.py (hipy.ir.Module.serialize).
/// ---------------------------------------------------------------------------------------------------------------------------

struct Type
{
    enum class Kind : uint8_t
    {
        Void,
        Bool,
        Int,
        Float,
        String,
        Record,
        Nullable,
        FunctionRef,
        Unsupported
    };
    Kind kind = Kind::Void;
    std::string name;
    /// Record members in the order of their names, as MakeRecord and RecordGet refer to them by name.
    std::vector<std::string> memberNames;
    std::vector<Type> memberTypes;
    /// The value type of a Nullable.
    std::vector<Type> inner;
};

enum class OpKind : uint8_t
{
    Constant,
    CallBuiltin,
    IfElse,
    Yield,
    Return,
    MakeRecord,
    RecordGet,
    FunctionRef,
    Call,
    CallIndirect,
    Undef,
    NoOp
};

struct Op
{
    OpKind kind = OpKind::NoOp;
    std::string name;
    std::vector<int64_t> args;
    int64_t result = -1;
    Type resultType;
    /// IfElse
    int64_t condition = -1;
    std::vector<int64_t> results;
    std::vector<Type> resultTypes;
    std::vector<Op> ifBody;
    std::vector<Op> elseBody;
    /// Constant
    nlohmann::json constant;
    std::string stringConstant;
    /// MakeRecord: member names, parallel to args. RecordGet: the member.
    std::vector<std::string> members;
    std::string member;
    /// FunctionRef: the closure, if any. CallIndirect: the function reference.
    int64_t closure = -1;
    int64_t fnref = -1;
};

struct Function
{
    std::string name;
    std::vector<int64_t> args;
    Type resultType;
    std::vector<Op> body;
};

struct Program
{
    std::unordered_map<std::string, Function> functions;
    std::string entry;
    size_t valueCount = 0;
    /// Values defined by an integer Constant, to emit a counting loop when the step of a range is a constant.
    std::unordered_map<int64_t, int64_t> intConstants;
};

namespace
{
const std::unordered_set<std::string_view> supportedBuiltins = {
    "undef",
    "error",
    "range.iter",
    "while.iter",
    "scalar.string.iter",
    "nullable.make",
    "nullable.null",
    "nullable.is_null",
    "nullable.get_value",
    "scalar.bool.and",
    "scalar.bool.or",
    "scalar.bool.not",
    "scalar.int.add",
    "scalar.int.sub",
    "scalar.int.mul",
    "scalar.int.div",
    "scalar.int.mod",
    "scalar.int.lshift",
    "scalar.int.and",
    "scalar.int.xor",
    "scalar.int.compare.eq",
    "scalar.int.compare.neq",
    "scalar.int.compare.lt",
    "scalar.int.compare.lte",
    "scalar.int.compare.gt",
    "scalar.int.compare.gte",
    "scalar.int.to_string",
    "scalar.int.from_string",
    "scalar.int.pyint_to_int64",
    "scalar.float.add",
    "scalar.float.sub",
    "scalar.float.mul",
    "scalar.float.div",
    "scalar.float.mod",
    "scalar.float.neg",
    "scalar.float.pow",
    "scalar.float.ceil",
    "scalar.float.exp",
    "scalar.float.log",
    "scalar.float.sqrt",
    "scalar.float.sin",
    "scalar.float.cos",
    "scalar.float.acos",
    "scalar.float.atan2",
    "scalar.float.copysign",
    "scalar.float.round",
    "scalar.float.compare.eq",
    "scalar.float.compare.neq",
    "scalar.float.compare.lt",
    "scalar.float.compare.lte",
    "scalar.float.compare.gt",
    "scalar.float.compare.gte",
    "scalar.float.from_int",
    "scalar.float.to_int",
    "scalar.float.to_string",
    "scalar.float.from_string",
    "scalar.string.compare.eq",
    "scalar.string.compare.lt",
    "scalar.string.compare.lte",
    "scalar.string.concatenate",
    "scalar.string.contains",
    "scalar.string.find",
    "scalar.string.rfind",
    "scalar.string.length",
    "scalar.string.lower",
    "scalar.string.upper",
    "scalar.string.ord",
    "scalar.string.replace",
    "scalar.string.strip",
    "scalar.string.substr",
    "scalar.string.at",
};

Type parseType(const nlohmann::json& json)
{
    Type type;
    type.name = json.at("name").get<std::string>();
    if (type.name == "void")
    {
        type.kind = Type::Kind::Void;
    }
    else if (type.name == "bool")
    {
        type.kind = Type::Kind::Bool;
    }
    else if (type.name == "int" || type.name == "pyint")
    {
        type.kind = Type::Kind::Int;
    }
    else if (type.name == "float")
    {
        type.kind = Type::Kind::Float;
    }
    else if (type.name == "string")
    {
        type.kind = Type::Kind::String;
    }
    else if (type.name == "record")
    {
        type.kind = Type::Kind::Record;
        for (const auto& [memberName, memberType] : json.at("members").items())
        {
            type.memberNames.push_back(memberName);
            type.memberTypes.push_back(parseType(memberType));
        }
    }
    else if (type.name == "nullable")
    {
        type.kind = Type::Kind::Nullable;
        type.inner.push_back(parseType(json.at("type")));
    }
    else if (type.name == "function_ref")
    {
        type.kind = Type::Kind::FunctionRef;
    }
    else
    {
        type.kind = Type::Kind::Unsupported;
    }
    return type;
}

class Parser
{
public:
    explicit Parser(Program& program) : program(program) { }

    int64_t value(const nlohmann::json& json)
    {
        const auto id = json.at("id").get<int64_t>();
        program.valueCount = std::max(program.valueCount, static_cast<size_t>(id) + 1);
        return id;
    }

    std::vector<int64_t> values(const nlohmann::json& json)
    {
        std::vector<int64_t> ids;
        for (const auto& element : json)
        {
            ids.push_back(value(element));
        }
        return ids;
    }

    std::vector<Op> block(const nlohmann::json& json)
    {
        std::vector<Op> ops;
        for (const auto& element : json)
        {
            ops.push_back(op(element));
        }
        return ops;
    }

    Op op(const nlohmann::json& json)
    {
        Op op;
        const auto kind = json.at("kind").get<std::string>();
        if (json.contains("result"))
        {
            op.result = value(json.at("result"));
            op.resultType = parseType(json.at("result").at("type"));
            checkType(op.resultType);
        }
        if (kind == "Constant")
        {
            op.kind = OpKind::Constant;
            op.constant = json.at("value");
            if (op.resultType.kind == Type::Kind::String)
            {
                op.stringConstant = op.constant.get<std::string>();
            }
            if (op.resultType.kind == Type::Kind::Int && op.constant.is_number_integer())
            {
                program.intConstants[op.result] = op.constant.get<int64_t>();
            }
        }
        else if (kind == "CallBuiltin")
        {
            op.kind = OpKind::CallBuiltin;
            op.name = json.at("name").get<std::string>();
            op.args = values(json.at("args"));
            if (!supportedBuiltins.contains(op.name))
            {
                unsupported.insert(op.name);
            }
        }
        else if (kind == "IfElse")
        {
            op.kind = OpKind::IfElse;
            op.condition = value(json.at("cond"));
            op.results = values(json.at("results"));
            for (const auto& result : json.at("results"))
            {
                op.resultTypes.push_back(parseType(result.at("type")));
                checkType(op.resultTypes.back());
                if (op.resultTypes.back().kind == Type::Kind::FunctionRef)
                {
                    unsupported.insert("if/else that yields a function reference");
                }
            }
            op.ifBody = block(json.at("ifBody"));
            op.elseBody = block(json.at("elseBody"));
        }
        else if (kind == "Yield" || kind == "Return")
        {
            op.kind = kind == "Yield" ? OpKind::Yield : OpKind::Return;
            op.args = values(json.at("values"));
        }
        else if (kind == "MakeRecord")
        {
            op.kind = OpKind::MakeRecord;
            for (const auto& [memberName, memberValue] : json.at("values").items())
            {
                op.members.push_back(memberName);
                op.args.push_back(value(memberValue));
            }
        }
        else if (kind == "RecordGet")
        {
            op.kind = OpKind::RecordGet;
            op.args = {value(json.at("record"))};
            op.member = json.at("member").get<std::string>();
        }
        else if (kind == "FunctionRef")
        {
            op.kind = OpKind::FunctionRef;
            op.name = json.at("name").get<std::string>();
            if (json.contains("closure"))
            {
                op.closure = value(json.at("closure"));
            }
        }
        else if (kind == "Call")
        {
            op.args = values(json.at("args"));
            if (json.contains("fnref"))
            {
                op.kind = OpKind::CallIndirect;
                op.fnref = value(json.at("fnref"));
            }
            else
            {
                op.kind = OpKind::Call;
                op.name = json.at("name").get<std::string>();
            }
        }
        else if (kind == "Undef")
        {
            op.kind = OpKind::Undef;
        }
        else if (kind == "NoOp")
        {
            op.kind = OpKind::NoOp;
        }
        else
        {
            unsupported.insert(kind);
        }
        return op;
    }

    void checkType(const Type& type)
    {
        if (type.kind == Type::Kind::Unsupported)
        {
            unsupported.insert("values of type " + type.name);
        }
        for (const auto& member : type.memberTypes)
        {
            checkType(member);
        }
        for (const auto& inner : type.inner)
        {
            checkType(inner);
        }
    }

    Program& program;
    std::unordered_set<std::string> unsupported;
};

std::shared_ptr<const Program> parseProgram(const std::string& text)
{
    auto program = std::make_shared<Program>();
    const auto json = nlohmann::json::parse(text);
    program->entry = json.at("entry").get<std::string>();
    Parser parser(*program);
    for (const auto& functionJson : json.at("module").at("functions"))
    {
        Function function;
        function.name = functionJson.at("name").get<std::string>();
        function.args = parser.values(functionJson.at("args"));
        function.resultType = parseType(functionJson.at("res_type"));
        parser.checkType(function.resultType);
        function.body = parser.block(functionJson.at("body"));
        program->functions.emplace(function.name, std::move(function));
    }
    if (!program->functions.contains(program->entry))
    {
        throw QueryCompilerError("HiPy UDF: the module has no function {}", program->entry);
    }
    if (!parser.unsupported.empty())
    {
        std::vector<std::string> names(parser.unsupported.begin(), parser.unsupported.end());
        std::ranges::sort(names);
        throw QueryCompilerError("HiPy UDF: the Nautilus backend does not support {}", fmt::join(names, ", "));
    }
    return program;
}

/// ---------------------------------------------------------------------------------------------------------------------------
/// Runtime functions, called with nautilus::invoke. Strings are UTF-8; lengths and indices count code points, as in LingoDB's
/// runtime for HiPy (StringRuntime::len, substr). A function that makes a string writes it to `result` and returns its size;
/// the caller allocates it in the arena and copies it with copyResult.
/// ---------------------------------------------------------------------------------------------------------------------------

thread_local std::string result; /// NOLINT(cppcoreguidelines-avoid-non-const-global-variables)

std::string_view view(const int8_t* data, const uint64_t size)
{
    return {reinterpret_cast<const char*>(data), size};
}

bool isContinuation(const char byte)
{
    return (static_cast<uint8_t>(byte) >> 6) == 2;
}

/// The byte offset of code point `index` (clamped to [0, code points]).
uint64_t byteOffset(const std::string_view text, const int64_t index)
{
    if (index <= 0)
    {
        return 0;
    }
    int64_t seen = 0;
    for (uint64_t offset = 0; offset < text.size(); ++offset)
    {
        if (!isContinuation(text[offset]))
        {
            if (seen == index)
            {
                return offset;
            }
            ++seen;
        }
    }
    return text.size();
}

int64_t codePointIndex(const std::string_view text, const uint64_t offset)
{
    return static_cast<int64_t>(std::ranges::count_if(text.substr(0, offset), [](const char byte) { return !isContinuation(byte); }));
}

uint64_t hipyByteOffset(const int8_t* data, const uint64_t size, const int64_t index)
{
    return byteOffset(view(data, size), index);
}

int64_t hipyLength(const int8_t* data, const uint64_t size)
{
    return codePointIndex(view(data, size), size);
}

uint64_t hipyCodePointSize(const int8_t* data, const uint64_t size, const uint64_t offset)
{
    auto end = offset + 1;
    while (end < size && isContinuation(static_cast<char>(data[end])))
    {
        ++end;
    }
    return end - offset;
}

int64_t hipyFind(const int8_t* data, const uint64_t size, const int8_t* needle, const uint64_t needleSize, const int64_t start, const int64_t end)
{
    const auto text = view(data, size);
    const auto from = byteOffset(text, start);
    const auto to = std::max(byteOffset(text, end), from);
    const auto position = text.substr(0, to).find(view(needle, needleSize), from);
    return position == std::string_view::npos ? -1 : codePointIndex(text, position);
}

int64_t hipyRFind(const int8_t* data, const uint64_t size, const int8_t* needle, const uint64_t needleSize, const int64_t start, const int64_t end)
{
    const auto text = view(data, size);
    const auto from = byteOffset(text, start);
    const auto to = std::max(byteOffset(text, end), from);
    const auto position = text.substr(from, to - from).rfind(view(needle, needleSize));
    return position == std::string_view::npos ? -1 : codePointIndex(text, from + position);
}

bool hipyContains(const int8_t* data, const uint64_t size, const int8_t* needle, const uint64_t needleSize)
{
    return view(data, size).find(view(needle, needleSize)) != std::string_view::npos;
}

int32_t hipyCompare(const int8_t* left, const uint64_t leftSize, const int8_t* right, const uint64_t rightSize)
{
    const auto order = view(left, leftSize).compare(view(right, rightSize));
    return order < 0 ? -1 : (order > 0 ? 1 : 0);
}

int64_t hipyOrd(const int8_t* data, const uint64_t size)
{
    if (size == 0)
    {
        throw UdfExecutionError("TypeError: ord() expected a character, but string of length 0 found");
    }
    const auto* bytes = reinterpret_cast<const uint8_t*>(data);
    const auto length = hipyCodePointSize(data, size, 0);
    if (length != size)
    {
        throw UdfExecutionError("TypeError: ord() expected a character, but a longer string was found");
    }
    if (length == 1)
    {
        return bytes[0];
    }
    int64_t code = bytes[0] & (0xFF >> (length + 1));
    for (uint64_t index = 1; index < length; ++index)
    {
        code = (code << 6) | (bytes[index] & 0x3F);
    }
    return code;
}

void hipyRaise(const int8_t* data, const uint64_t size)
{
    throw UdfExecutionError("HiPy UDF raised: {}", view(data, size));
}

void hipyRaiseZeroDivision()
{
    throw UdfExecutionError("ZeroDivisionError: integer division or modulo by zero");
}

void hipyCopyResult(int8_t* destination)
{
    std::memcpy(destination, result.data(), result.size());
}

uint64_t hipyLower(const int8_t* data, const uint64_t size)
{
    result.assign(view(data, size));
    std::ranges::transform(result, result.begin(), [](const char byte) { return (byte >= 'A' && byte <= 'Z') ? byte + 32 : byte; });
    return result.size();
}

uint64_t hipyUpper(const int8_t* data, const uint64_t size)
{
    result.assign(view(data, size));
    std::ranges::transform(result, result.begin(), [](const char byte) { return (byte >= 'a' && byte <= 'z') ? byte - 32 : byte; });
    return result.size();
}

constexpr std::string_view whitespace = " \t\n\r\f\v";

std::string_view strip(std::string_view text)
{
    const auto begin = text.find_first_not_of(whitespace);
    if (begin == std::string_view::npos)
    {
        return {};
    }
    return text.substr(begin, text.find_last_not_of(whitespace) - begin + 1);
}

/// strip() returns a part of its argument: the result is its offset and size.
uint64_t hipyStripOffset(const int8_t* data, const uint64_t size)
{
    const auto text = view(data, size);
    const auto stripped = strip(text);
    return stripped.empty() ? 0 : static_cast<uint64_t>(stripped.data() - text.data());
}

uint64_t hipyStripSize(const int8_t* data, const uint64_t size)
{
    return strip(view(data, size)).size();
}

uint64_t hipyReplace(
    const int8_t* data, const uint64_t size, const int8_t* old, const uint64_t oldSize, const int8_t* replacement, const uint64_t replacementSize)
{
    const auto text = view(data, size);
    const auto from = view(old, oldSize);
    const auto to = view(replacement, replacementSize);
    result.clear();
    if (from.empty())
    {
        /// Python inserts the replacement before every code point and at the end.
        for (uint64_t offset = 0; offset < text.size();)
        {
            const auto length = hipyCodePointSize(data, size, offset);
            result.append(to);
            result.append(text.substr(offset, length));
            offset += length;
        }
        result.append(to);
        return result.size();
    }
    size_t position = 0;
    for (auto found = text.find(from); found != std::string_view::npos; found = text.find(from, position))
    {
        result.append(text.substr(position, found - position));
        result.append(to);
        position = found + from.size();
    }
    result.append(text.substr(position));
    return result.size();
}

uint64_t hipyIntToString(const int64_t value)
{
    result = std::to_string(value);
    return result.size();
}

/// Python's repr of a float: the shortest digits that read back as the same value, in fixed notation for exponents in
/// [-4, 16) with at least one decimal, else in scientific notation with a two-digit exponent.
uint64_t hipyFloatToString(const double value)
{
    if (std::isnan(value))
    {
        result = "nan";
        return result.size();
    }
    if (std::isinf(value))
    {
        result = value < 0 ? "-inf" : "inf";
        return result.size();
    }
    std::array<char, 64> buffer{};
    const auto [end, error] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value, std::chars_format::scientific);
    INVARIANT(error == std::errc{}, "to_chars of a double does not fail with 64 bytes");
    const std::string_view scientific(buffer.data(), end);
    const auto exponentAt = scientific.find('e');
    const auto negative = scientific.front() == '-';
    std::string digits;
    for (const char character : scientific.substr(negative ? 1 : 0, exponentAt - (negative ? 1 : 0)))
    {
        if (character != '.')
        {
            digits += character;
        }
    }
    int exponent = 0;
    std::from_chars(scientific.data() + exponentAt + (scientific[exponentAt + 1] == '+' ? 2 : 1), scientific.data() + scientific.size(), exponent);
    result = negative ? "-" : "";
    if (exponent >= -4 && exponent < 16)
    {
        if (exponent < 0)
        {
            result += "0." + std::string(static_cast<size_t>(-exponent - 1), '0') + digits;
        }
        else if (static_cast<size_t>(exponent) + 1 >= digits.size())
        {
            result += digits + std::string(static_cast<size_t>(exponent) + 1 - digits.size(), '0') + ".0";
        }
        else
        {
            result += digits.substr(0, static_cast<size_t>(exponent) + 1) + "." + digits.substr(static_cast<size_t>(exponent) + 1);
        }
    }
    else
    {
        result += digits.substr(0, 1);
        if (digits.size() > 1)
        {
            result += "." + digits.substr(1);
        }
        result += fmt::format("e{}{:02}", exponent < 0 ? '-' : '+', std::abs(exponent));
    }
    return result.size();
}

int64_t hipyIntFromString(const int8_t* data, const uint64_t size)
{
    auto text = std::string(strip(view(data, size)));
    std::erase(text, '_');
    const auto* begin = text.data();
    if (!text.empty() && text.front() == '+')
    {
        ++begin;
    }
    int64_t value = 0;
    const auto [end, error] = std::from_chars(begin, text.data() + text.size(), value);
    if (text.empty() || error != std::errc{} || end != text.data() + text.size())
    {
        throw UdfExecutionError("ValueError: invalid literal for int() with base 10: '{}'", view(data, size));
    }
    return value;
}

double hipyFloatFromString(const int8_t* data, const uint64_t size)
{
    const auto text = std::string(strip(view(data, size)));
    char* end = nullptr;
    const auto value = std::strtod(text.c_str(), &end);
    if (text.empty() || end != text.c_str() + text.size())
    {
        throw UdfExecutionError("ValueError: could not convert string to float: '{}'", view(data, size));
    }
    return value;
}

double roundHalfEven(const double value)
{
    const auto rounded = std::round(value);
    if (std::abs(value - std::trunc(value)) == 0.5)
    {
        return 2.0 * std::round(value / 2.0);
    }
    return rounded;
}

double hipyRound(const double value, const int64_t digits)
{
    if (!std::isfinite(value) || value == 0.0)
    {
        return value;
    }
    if (digits == 0)
    {
        return roundHalfEven(value);
    }
    if (digits > 308)
    {
        return value;
    }
    if (digits < -308)
    {
        return std::copysign(0.0, value);
    }
    const auto scale = std::pow(10.0, static_cast<double>(digits));
    const auto scaled = value * scale;
    if (!std::isfinite(scaled) || scale == 0.0)
    {
        return digits > 0 ? value : std::copysign(0.0, value);
    }
    return roundHalfEven(scaled) / scale;
}

/// Python's float %: the remainder has the sign of the divisor.
double hipyFmod(const double left, const double right)
{
    if (right == 0.0)
    {
        throw UdfExecutionError("ZeroDivisionError: float modulo");
    }
    auto remainder = std::fmod(left, right);
    if (remainder != 0.0 && ((remainder < 0.0) != (right < 0.0)))
    {
        remainder += right;
    }
    else if (remainder == 0.0)
    {
        remainder = std::copysign(0.0, right);
    }
    return remainder;
}
double hipyPow(const double left, const double right)
{
    return std::pow(left, right);
}
double hipyCeil(const double value)
{
    return std::ceil(value);
}
double hipyExp(const double value)
{
    return std::exp(value);
}
double hipyLog(const double value)
{
    return std::log(value);
}
double hipySqrt(const double value)
{
    return std::sqrt(value);
}
double hipySin(const double value)
{
    return std::sin(value);
}
double hipyCos(const double value)
{
    return std::cos(value);
}
double hipyAcos(const double value)
{
    return std::acos(value);
}
double hipyAtan2(const double left, const double right)
{
    return std::atan2(left, right);
}
double hipyCopysign(const double left, const double right)
{
    return std::copysign(left, right);
}

/// ---------------------------------------------------------------------------------------------------------------------------
/// The interpreter. It runs at trace time on Nautilus values, so each operation becomes Nautilus operations in the pipeline.
/// ---------------------------------------------------------------------------------------------------------------------------

using nautilus::val;
struct RecordValue;
struct NullableValue;
struct FunctionRefValue;
using Value = std::variant<
    std::monostate,
    val<bool>,
    val<int64_t>,
    val<double>,
    VariableSizedData,
    std::shared_ptr<RecordValue>,
    std::shared_ptr<NullableValue>,
    std::shared_ptr<FunctionRefValue>>;

struct RecordValue
{
    std::vector<std::pair<std::string, Value>> members;
};

struct NullableValue
{
    Value value;
    val<bool> isNull;
};

struct FunctionRefValue
{
    std::string name;
    Value closure;
};

constexpr size_t maximumCallDepth = 64;

class Interpreter
{
public:
    Interpreter(const Program& program, ArenaRef& arena) : program(program), arena(arena) { }

    Value call(const std::string& name, std::vector<Value> arguments)
    {
        const auto found = program.functions.find(name);
        if (found == program.functions.end())
        {
            throw QueryCompilerError("HiPy UDF: call of unknown function {}", name);
        }
        const auto& function = found->second;
        if (depth >= maximumCallDepth)
        {
            throw QueryCompilerError("HiPy UDF: calls nest deeper than {} (recursion is not supported)", maximumCallDepth);
        }
        if (arguments.size() != function.args.size())
        {
            throw QueryCompilerError("HiPy UDF: {} takes {} arguments, got {}", name, function.args.size(), arguments.size());
        }
        ++depth;
        std::vector<Value> environment(program.valueCount);
        for (nautilus::static_val<size_t> index = 0; index < arguments.size(); ++index)
        {
            environment[function.args[index]] = std::move(arguments[index]);
        }
        auto results = run(function.body, environment);
        --depth;
        return results.empty() ? Value{} : std::move(results.front());
    }

    /// A Value for the type with fresh Nautilus variables, which an if/else or a loop assigns.
    static Value placeholder(const Type& type)
    {
        switch (type.kind)
        {
            case Type::Kind::Bool:
                return val<bool>(false);
            case Type::Kind::Int:
                return val<int64_t>(0);
            case Type::Kind::Float:
                return val<double>(0.0);
            case Type::Kind::String:
                return VariableSizedData(val<int8_t*>(nullptr), val<uint64_t>(0));
            case Type::Kind::Record: {
                auto record = std::make_shared<RecordValue>();
                for (nautilus::static_val<size_t> index = 0; index < type.memberNames.size(); ++index)
                {
                    record->members.emplace_back(type.memberNames[index], placeholder(type.memberTypes[index]));
                }
                return record;
            }
            case Type::Kind::Nullable:
                return std::make_shared<NullableValue>(NullableValue{.value = placeholder(type.inner.front()), .isNull = val<bool>(true)});
            case Type::Kind::Void:
                return std::monostate{};
            case Type::Kind::FunctionRef:
            case Type::Kind::Unsupported:
                break;
        }
        throw QueryCompilerError("HiPy UDF: no placeholder for a value of type {}", type.name);
    }

    /// A copy of the value in new Nautilus variables, so that assigning the copy does not change the value.
    static Value fresh(const Value& value)
    {
        return std::visit(
            []<typename T>(const T& alternative) -> Value
            {
                if constexpr (std::is_same_v<T, std::shared_ptr<RecordValue>>)
                {
                    auto record = std::make_shared<RecordValue>();
                    for (nautilus::static_val<size_t> index = 0; index < alternative->members.size(); ++index)
                    {
                        const auto& [name, member] = alternative->members[index];
                        record->members.emplace_back(name, fresh(member));
                    }
                    return record;
                }
                else if constexpr (std::is_same_v<T, std::shared_ptr<NullableValue>>)
                {
                    return std::make_shared<NullableValue>(NullableValue{.value = fresh(alternative->value), .isNull = alternative->isNull});
                }
                else if constexpr (std::is_same_v<T, std::shared_ptr<FunctionRefValue>>)
                {
                    return std::make_shared<FunctionRefValue>(FunctionRefValue{.name = alternative->name, .closure = fresh(alternative->closure)});
                }
                else if constexpr (std::is_same_v<T, VariableSizedData>)
                {
                    return VariableSizedData(alternative.getContent(), alternative.getSize());
                }
                else
                {
                    return T(alternative);
                }
            },
            value);
    }

    /// Assigns the Nautilus variables of `target` from `source`, member by member.
    static void assign(Value& target, const Value& source)
    {
        if (target.index() != source.index())
        {
            throw QueryCompilerError("HiPy UDF: a branch or loop assigns a value of a different kind");
        }
        std::visit(
            [&source]<typename T>(T& alternative)
            {
                const auto& from = std::get<T>(source);
                if constexpr (std::is_same_v<T, std::shared_ptr<RecordValue>>)
                {
                    for (nautilus::static_val<size_t> index = 0; index < alternative->members.size(); ++index)
                    {
                        assign(alternative->members[index].second, from->members.at(index).second);
                    }
                }
                else if constexpr (std::is_same_v<T, std::shared_ptr<NullableValue>>)
                {
                    assign(alternative->value, from->value);
                    alternative->isNull = from->isNull;
                }
                else if constexpr (std::is_same_v<T, std::shared_ptr<FunctionRefValue>>)
                {
                    if (alternative->name != from->name)
                    {
                        throw QueryCompilerError("HiPy UDF: a branch or loop changes a function reference");
                    }
                    assign(alternative->closure, from->closure);
                }
                else if constexpr (!std::is_same_v<T, std::monostate>)
                {
                    alternative = from;
                }
            },
            target);
    }

private:
    template <typename T>
    static T& as(std::vector<Value>& environment, const int64_t id)
    {
        auto* value = std::get_if<T>(&environment[id]);
        if (value == nullptr)
        {
            throw QueryCompilerError("HiPy UDF: value %{} has an unexpected kind", id);
        }
        return *value;
    }

    static val<int64_t>& integer(std::vector<Value>& environment, const int64_t id) { return as<val<int64_t>>(environment, id); }
    static val<double>& real(std::vector<Value>& environment, const int64_t id) { return as<val<double>>(environment, id); }
    static val<bool>& boolean(std::vector<Value>& environment, const int64_t id) { return as<val<bool>>(environment, id); }
    static VariableSizedData& string(std::vector<Value>& environment, const int64_t id) { return as<VariableSizedData>(environment, id); }

    VariableSizedData takeResult(const val<uint64_t>& size)
    {
        auto data = arena.allocateVariableSizedData(size);
        nautilus::invoke(hipyCopyResult, data.getContent());
        return data;
    }

    Value callReference(const Value& reference, std::vector<Value> arguments)
    {
        const auto* function = std::get_if<std::shared_ptr<FunctionRefValue>>(&reference);
        if (function == nullptr)
        {
            throw QueryCompilerError("HiPy UDF: call of a value that is not a function reference");
        }
        if (!std::holds_alternative<std::monostate>((*function)->closure))
        {
            arguments.push_back((*function)->closure);
        }
        return call((*function)->name, std::move(arguments));
    }

    std::vector<Value> run(const std::vector<Op>& block, std::vector<Value>& environment)
    {
        /// The op index is a static value of the trace, so that ops that run the same C++ code (two if/else in a block)
        /// get different trace tags; otherwise Nautilus takes the second for a loop back to the first.
        for (nautilus::static_val<size_t> opIndex = 0; opIndex < block.size(); ++opIndex)
        {
            const auto& op = block[opIndex];
            switch (op.kind)
            {
                case OpKind::Return:
                case OpKind::Yield: {
                    std::vector<Value> results;
                    for (nautilus::static_val<size_t> index = 0; index < op.args.size(); ++index)
                    {
                        results.push_back(environment[op.args[index]]);
                    }
                    return results;
                }
                case OpKind::Constant:
                    environment[op.result] = constant(op);
                    break;
                case OpKind::Undef:
                    environment[op.result] = placeholder(op.resultType);
                    break;
                case OpKind::NoOp:
                    break;
                case OpKind::MakeRecord: {
                    auto record = std::make_shared<RecordValue>();
                    for (nautilus::static_val<size_t> index = 0; index < op.resultType.memberNames.size(); ++index)
                    {
                        const auto& name = op.resultType.memberNames[index];
                        const auto position = std::ranges::find(op.members, name) - op.members.begin();
                        record->members.emplace_back(name, environment[op.args.at(position)]);
                    }
                    environment[op.result] = std::move(record);
                    break;
                }
                case OpKind::RecordGet: {
                    const auto& record = as<std::shared_ptr<RecordValue>>(environment, op.args.front());
                    const auto member = std::ranges::find(record->members, op.member, &std::pair<std::string, Value>::first);
                    if (member == record->members.end())
                    {
                        throw QueryCompilerError("HiPy UDF: record has no member {}", op.member);
                    }
                    environment[op.result] = member->second;
                    break;
                }
                case OpKind::FunctionRef:
                    environment[op.result] = std::make_shared<FunctionRefValue>(
                        FunctionRefValue{.name = op.name, .closure = op.closure >= 0 ? environment[op.closure] : Value{}});
                    break;
                case OpKind::Call: {
                    std::vector<Value> arguments;
                    for (nautilus::static_val<size_t> index = 0; index < op.args.size(); ++index)
                    {
                        arguments.push_back(environment[op.args[index]]);
                    }
                    environment[op.result] = call(op.name, std::move(arguments));
                    break;
                }
                case OpKind::CallIndirect: {
                    std::vector<Value> arguments;
                    for (nautilus::static_val<size_t> index = 0; index < op.args.size(); ++index)
                    {
                        arguments.push_back(environment[op.args[index]]);
                    }
                    environment[op.result] = callReference(environment[op.fnref], std::move(arguments));
                    break;
                }
                case OpKind::IfElse: {
                    std::vector<Value> results;
                    for (nautilus::static_val<size_t> index = 0; index < op.resultTypes.size(); ++index)
                    {
                        results.push_back(placeholder(op.resultTypes[index]));
                    }
                    if (boolean(environment, op.condition))
                    {
                        const auto yielded = run(op.ifBody, environment);
                        for (nautilus::static_val<size_t> index = 0; index < results.size(); ++index)
                        {
                            assign(results[index], yielded.at(index));
                        }
                    }
                    else
                    {
                        const auto yielded = run(op.elseBody, environment);
                        for (nautilus::static_val<size_t> index = 0; index < results.size(); ++index)
                        {
                            assign(results[index], yielded.at(index));
                        }
                    }
                    for (nautilus::static_val<size_t> index = 0; index < results.size(); ++index)
                    {
                        environment[op.results[index]] = std::move(results[index]);
                    }
                    break;
                }
                case OpKind::CallBuiltin:
                    environment[op.result] = builtin(op, environment);
                    break;
            }
        }
        return {};
    }

    static Value constant(const Op& op)
    {
        switch (op.resultType.kind)
        {
            case Type::Kind::Bool:
                return val<bool>(op.constant.get<bool>());
            case Type::Kind::Int:
                return val<int64_t>(op.constant.is_boolean() ? static_cast<int64_t>(op.constant.get<bool>()) : op.constant.get<int64_t>());
            case Type::Kind::Float:
                return val<double>(op.constant.get<double>());
            case Type::Kind::String:
                /// The program owns the text for as long as the pipeline runs, like ConstantValueVariableSizePhysicalFunction.
                return VariableSizedData(
                    val<int8_t*>(const_cast<int8_t*>(reinterpret_cast<const int8_t*>(op.stringConstant.data()))), /// NOLINT
                    val<uint64_t>(op.stringConstant.size()));
            case Type::Kind::Void:
                return std::monostate{};
            default:
                throw QueryCompilerError("HiPy UDF: constant of type {}", op.resultType.name);
        }
    }

    Value builtin(const Op& op, std::vector<Value>& environment)
    {
        const auto& name = op.name;
        const auto& a = op.args;
        auto& env = environment;
        /// Integers: C semantics, as the MLIR backend (arith.divsi, arith.remsi); HiPy's int implements Python's floor division.
        if (name == "scalar.int.add")
        {
            return integer(env, a[0]) + integer(env, a[1]);
        }
        if (name == "scalar.int.sub")
        {
            return integer(env, a[0]) - integer(env, a[1]);
        }
        if (name == "scalar.int.mul")
        {
            return integer(env, a[0]) * integer(env, a[1]);
        }
        if (name == "scalar.int.div" || name == "scalar.int.mod")
        {
            const auto& divisor = integer(env, a[1]);
            if (divisor == val<int64_t>(0))
            {
                nautilus::invoke(hipyRaiseZeroDivision);
            }
            const val<int64_t> safe = nautilus::select(divisor == val<int64_t>(0), val<int64_t>(1), divisor);
            /// Python's // and % (HiPy's int.__floordiv__ and __mod__): the quotient rounds toward negative infinity, and the
            /// remainder has the sign of the divisor.
            const val<int64_t> quotient = integer(env, a[0]) / safe;
            const val<int64_t> remainder = integer(env, a[0]) % safe;
            const val<bool> adjust = remainder != val<int64_t>(0) && ((remainder < val<int64_t>(0)) != (safe < val<int64_t>(0)));
            if (name == "scalar.int.div")
            {
                return nautilus::select(adjust, val<int64_t>(quotient - 1), quotient);
            }
            return nautilus::select(adjust, val<int64_t>(remainder + safe), remainder);
        }
        if (name == "scalar.int.lshift")
        {
            return integer(env, a[0]) << integer(env, a[1]);
        }
        if (name == "scalar.int.and")
        {
            return integer(env, a[0]) & integer(env, a[1]);
        }
        if (name == "scalar.int.xor")
        {
            return integer(env, a[0]) ^ integer(env, a[1]);
        }
        if (name.starts_with("scalar.int.compare."))
        {
            return compare(name.substr(std::string_view("scalar.int.compare.").size()), integer(env, a[0]), integer(env, a[1]));
        }
        if (name == "scalar.int.pyint_to_int64")
        {
            return integer(env, a[0]);
        }
        if (name == "scalar.int.to_string")
        {
            return takeResult(nautilus::invoke(hipyIntToString, integer(env, a[0])));
        }
        if (name == "scalar.int.from_string")
        {
            const auto& text = string(env, a[0]);
            return nautilus::invoke(hipyIntFromString, text.getContent(), text.getSize());
        }

        /// Floats
        if (name == "scalar.float.add")
        {
            return real(env, a[0]) + real(env, a[1]);
        }
        if (name == "scalar.float.sub")
        {
            return real(env, a[0]) - real(env, a[1]);
        }
        if (name == "scalar.float.mul")
        {
            return real(env, a[0]) * real(env, a[1]);
        }
        if (name == "scalar.float.div")
        {
            return real(env, a[0]) / real(env, a[1]);
        }
        if (name == "scalar.float.neg")
        {
            return val<double>(0.0) - real(env, a[0]);
        }
        if (name.starts_with("scalar.float.compare."))
        {
            return compare(name.substr(std::string_view("scalar.float.compare.").size()), real(env, a[0]), real(env, a[1]));
        }
        if (name == "scalar.float.from_int")
        {
            return static_cast<val<double>>(integer(env, a[0]));
        }
        if (name == "scalar.float.to_int")
        {
            return static_cast<val<int64_t>>(real(env, a[0]));
        }
        if (name == "scalar.float.to_string")
        {
            return takeResult(nautilus::invoke(hipyFloatToString, real(env, a[0])));
        }
        if (name == "scalar.float.from_string")
        {
            const auto& text = string(env, a[0]);
            return nautilus::invoke(hipyFloatFromString, text.getContent(), text.getSize());
        }
        if (name == "scalar.float.round")
        {
            const val<double> rounded = nautilus::invoke(hipyRound, real(env, a[0]), integer(env, a[1]));
            return op.resultType.kind == Type::Kind::Int ? Value{static_cast<val<int64_t>>(rounded)} : Value{rounded};
        }
        if (name == "scalar.float.ceil")
        {
            const val<double> rounded = nautilus::invoke(hipyCeil, real(env, a[0]));
            return op.resultType.kind == Type::Kind::Int ? Value{static_cast<val<int64_t>>(rounded)} : Value{rounded};
        }
        using Binary = double (*)(double, double);
        using Unary = double (*)(double);
        static const std::unordered_map<std::string_view, Binary> binaryMath
            = {{"scalar.float.mod", hipyFmod},
               {"scalar.float.pow", hipyPow},
               {"scalar.float.atan2", hipyAtan2},
               {"scalar.float.copysign", hipyCopysign}};
        static const std::unordered_map<std::string_view, Unary> unaryMath
            = {{"scalar.float.exp", hipyExp},
               {"scalar.float.log", hipyLog},
               {"scalar.float.sqrt", hipySqrt},
               {"scalar.float.sin", hipySin},
               {"scalar.float.cos", hipyCos},
               {"scalar.float.acos", hipyAcos}};
        if (const auto found = binaryMath.find(name); found != binaryMath.end())
        {
            return nautilus::invoke(found->second, real(env, a[0]), real(env, a[1]));
        }
        if (const auto found = unaryMath.find(name); found != unaryMath.end())
        {
            return nautilus::invoke(found->second, real(env, a[0]));
        }

        /// Booleans
        if (name == "scalar.bool.and")
        {
            return boolean(env, a[0]) && boolean(env, a[1]);
        }
        if (name == "scalar.bool.or")
        {
            return boolean(env, a[0]) || boolean(env, a[1]);
        }
        if (name == "scalar.bool.not")
        {
            return !boolean(env, a[0]);
        }

        /// Strings
        if (name.starts_with("scalar.string."))
        {
            return stringBuiltin(op, environment);
        }

        /// Nullable values
        if (name == "nullable.make")
        {
            return std::make_shared<NullableValue>(NullableValue{.value = env[a[0]], .isNull = val<bool>(false)});
        }
        if (name == "nullable.null")
        {
            return placeholder(op.resultType);
        }
        if (name == "nullable.is_null")
        {
            return val<bool>(as<std::shared_ptr<NullableValue>>(env, a[0])->isNull);
        }
        if (name == "nullable.get_value")
        {
            const auto& nullable = as<std::shared_ptr<NullableValue>>(env, a[0]);
            if (nullable->isNull)
            {
                static constexpr std::string_view message = "get_value() on NULL";
                nautilus::invoke(
                    hipyRaise,
                    val<int8_t*>(const_cast<int8_t*>(reinterpret_cast<const int8_t*>(message.data()))), /// NOLINT
                    val<uint64_t>(message.size()));
            }
            return nullable->value;
        }

        if (name == "error")
        {
            const auto& message = string(env, a[0]);
            nautilus::invoke(hipyRaise, message.getContent(), message.getSize());
            return val<bool>(true);
        }
        if (name == "undef")
        {
            return placeholder(op.resultType);
        }

        /// Loops: the state is a record that the loop function returns for the next iteration.
        if (name == "range.iter")
        {
            auto state = fresh(env[a[2]]);
            const auto& closure = env[a[1]];
            const val<int64_t> end = integer(env, a[4]);
            const val<int64_t> step = integer(env, a[5]);
            const auto constantStep = program.intConstants.find(a[5]);
            if (constantStep != program.intConstants.end() && constantStep->second > 0)
            {
                for (val<int64_t> index = integer(env, a[3]); index < end; index = index + step)
                {
                    auto next = callReference(env[a[0]], {closure, state, Value{index}});
                    assign(state, next);
                }
            }
            else
            {
                if (step == val<int64_t>(0))
                {
                    static constexpr std::string_view message = "ValueError: range() arg 3 must not be zero";
                    nautilus::invoke(
                        hipyRaise,
                        val<int8_t*>(const_cast<int8_t*>(reinterpret_cast<const int8_t*>(message.data()))), /// NOLINT
                        val<uint64_t>(message.size()));
                }
                for (val<int64_t> index = integer(env, a[3]); (step > val<int64_t>(0) && index < end) || (step < val<int64_t>(0) && index > end);
                     index = index + step)
                {
                    auto next = callReference(env[a[0]], {closure, state, Value{index}});
                    assign(state, next);
                }
            }
            return state;
        }
        if (name == "while.iter")
        {
            auto state = fresh(env[a[3]]);
            const auto& closure = env[a[2]];
            while (std::get<val<bool>>(callReference(env[a[0]], {closure, state})))
            {
                auto next = callReference(env[a[1]], {closure, state});
                assign(state, next);
            }
            return state;
        }
        throw QueryCompilerError("HiPy UDF: the Nautilus backend does not support {}", name);
    }

    Value stringBuiltin(const Op& op, std::vector<Value>& env)
    {
        const auto& name = op.name;
        const auto& a = op.args;
        if (name == "scalar.string.iter")
        {
            return iterateString(op, env);
        }
        const auto& text = string(env, a[0]);
        if (name == "scalar.string.length")
        {
            return nautilus::invoke(hipyLength, text.getContent(), text.getSize());
        }
        if (name == "scalar.string.substr" || name == "scalar.string.at")
        {
            const val<int64_t> start = integer(env, a[1]);
            const val<int64_t> stop = name == "scalar.string.at" ? val<int64_t>(start + 1) : val<int64_t>(integer(env, a[2]));
            const val<uint64_t> from = nautilus::invoke(hipyByteOffset, text.getContent(), text.getSize(), start);
            const val<uint64_t> to = nautilus::invoke(hipyByteOffset, text.getContent(), text.getSize(), stop);
            const val<uint64_t> size = nautilus::select(to > from, val<uint64_t>(to - from), val<uint64_t>(0));
            return VariableSizedData(text.getContent() + from, size);
        }
        if (name == "scalar.string.concatenate")
        {
            const auto& right = string(env, a[1]);
            auto data = arena.allocateVariableSizedData(text.getSize() + right.getSize());
            nautilus::memcpy(data.getContent(), text.getContent(), text.getSize());
            nautilus::memcpy(data.getContent() + text.getSize(), right.getContent(), right.getSize());
            return data;
        }
        if (name == "scalar.string.compare.eq")
        {
            return val<bool>(text == string(env, a[1]));
        }
        if (name == "scalar.string.compare.lt" || name == "scalar.string.compare.lte")
        {
            const auto& right = string(env, a[1]);
            const val<int32_t> order = nautilus::invoke(hipyCompare, text.getContent(), text.getSize(), right.getContent(), right.getSize());
            return name == "scalar.string.compare.lt" ? val<bool>(order < 0) : val<bool>(order <= 0);
        }
        if (name == "scalar.string.contains")
        {
            const auto& needle = string(env, a[1]);
            return nautilus::invoke(hipyContains, text.getContent(), text.getSize(), needle.getContent(), needle.getSize());
        }
        if (name == "scalar.string.find" || name == "scalar.string.rfind")
        {
            const auto& needle = string(env, a[1]);
            return nautilus::invoke(
                name == "scalar.string.find" ? hipyFind : hipyRFind,
                text.getContent(),
                text.getSize(),
                needle.getContent(),
                needle.getSize(),
                integer(env, a[2]),
                integer(env, a[3]));
        }
        if (name == "scalar.string.ord")
        {
            return nautilus::invoke(hipyOrd, text.getContent(), text.getSize());
        }
        if (name == "scalar.string.lower")
        {
            return takeResult(nautilus::invoke(hipyLower, text.getContent(), text.getSize()));
        }
        if (name == "scalar.string.upper")
        {
            return takeResult(nautilus::invoke(hipyUpper, text.getContent(), text.getSize()));
        }
        if (name == "scalar.string.strip")
        {
            const val<uint64_t> offset = nautilus::invoke(hipyStripOffset, text.getContent(), text.getSize());
            const val<uint64_t> size = nautilus::invoke(hipyStripSize, text.getContent(), text.getSize());
            return VariableSizedData(text.getContent() + offset, size);
        }
        if (name == "scalar.string.replace")
        {
            const auto& old = string(env, a[1]);
            const auto& replacement = string(env, a[2]);
            return takeResult(nautilus::invoke(
                hipyReplace, text.getContent(), text.getSize(), old.getContent(), old.getSize(), replacement.getContent(), replacement.getSize()));
        }
        throw QueryCompilerError("HiPy UDF: the Nautilus backend does not support {}", name);
    }

    /// iter(fnref, closure, init, string): calls the function on each code point.
    Value iterateString(const Op& op, std::vector<Value>& env)
    {
        const auto& a = op.args;
        {
            const auto& iterated = string(env, a[3]);
            auto state = fresh(env[a[2]]);
            const auto& closure = env[a[1]];
            const val<int8_t*> content = iterated.getContent();
            const val<uint64_t> size = iterated.getSize();
            for (val<uint64_t> offset = 0; offset < size;)
            {
                const val<uint64_t> length = nautilus::invoke(hipyCodePointSize, content, size, offset);
                auto next = callReference(env[a[0]], {closure, state, Value{VariableSizedData(content + offset, length)}});
                assign(state, next);
                offset = offset + length;
            }
            return state;
        }
    }

    template <typename T>
    static val<bool> compare(const std::string_view predicate, const val<T>& left, const val<T>& right)
    {
        if (predicate == "eq")
        {
            return left == right;
        }
        if (predicate == "neq")
        {
            return left != right;
        }
        if (predicate == "lt")
        {
            return left < right;
        }
        if (predicate == "lte")
        {
            return left <= right;
        }
        if (predicate == "gt")
        {
            return left > right;
        }
        if (predicate == "gte")
        {
            return left >= right;
        }
        throw QueryCompilerError("HiPy UDF: unknown comparison {}", predicate);
    }

    const Program& program;
    ArenaRef& arena;
    size_t depth = 0;
};

/// The UDF's argument as a value of HiPy's type: ints as int64, floats as double.
Value argumentValue(const VarVal& value, const DataType& type)
{
    switch (type.type)
    {
        case DataType::Type::VARSIZED:
            return value.getRawValueAs<VariableSizedData>();
        case DataType::Type::BOOLEAN:
            return value.getRawValueAs<val<bool>>();
        case DataType::Type::FLOAT32:
        case DataType::Type::FLOAT64:
            return value.castToType(DataType::Type::FLOAT64).getRawValueAs<val<double>>();
        case DataType::Type::INT8:
        case DataType::Type::INT16:
        case DataType::Type::INT32:
        case DataType::Type::INT64:
        case DataType::Type::UINT8:
        case DataType::Type::UINT16:
        case DataType::Type::UINT32:
        case DataType::Type::UINT64:
            return value.castToType(DataType::Type::INT64).getRawValueAs<val<int64_t>>();
        default:
            throw QueryCompilerError("HiPy UDF: arguments of type {} are not supported", type);
    }
}

/// The result as a nullable VarVal of the declared return type.
VarVal resultValue(const Value& value, const DataType& type, const val<bool>& isNull)
{
    if (const auto* string = std::get_if<VariableSizedData>(&value))
    {
        if (type.type != DataType::Type::VARSIZED)
        {
            throw QueryCompilerError("HiPy UDF returns a string, but declares {}", type);
        }
        return VarVal(*string, true, isNull);
    }
    const auto raw = std::visit(
        []<typename T>(const T& alternative) -> VarVal
        {
            if constexpr (std::is_same_v<T, val<bool>> || std::is_same_v<T, val<int64_t>> || std::is_same_v<T, val<double>>)
            {
                return VarVal(alternative);
            }
            else
            {
                throw QueryCompilerError("HiPy UDF returns a value that is not a scalar");
            }
        },
        value);
    const auto cast = raw.castToType(type.type);
    switch (type.type)
    {
        case DataType::Type::BOOLEAN:
            return {cast.getRawValueAs<val<bool>>(), true, isNull};
        case DataType::Type::INT8:
            return {cast.getRawValueAs<val<int8_t>>(), true, isNull};
        case DataType::Type::INT16:
            return {cast.getRawValueAs<val<int16_t>>(), true, isNull};
        case DataType::Type::INT32:
            return {cast.getRawValueAs<val<int32_t>>(), true, isNull};
        case DataType::Type::INT64:
            return {cast.getRawValueAs<val<int64_t>>(), true, isNull};
        case DataType::Type::UINT8:
            return {cast.getRawValueAs<val<uint8_t>>(), true, isNull};
        case DataType::Type::UINT16:
            return {cast.getRawValueAs<val<uint16_t>>(), true, isNull};
        case DataType::Type::UINT32:
            return {cast.getRawValueAs<val<uint32_t>>(), true, isNull};
        case DataType::Type::UINT64:
            return {cast.getRawValueAs<val<uint64_t>>(), true, isNull};
        case DataType::Type::FLOAT32:
            return {cast.getRawValueAs<val<float>>(), true, isNull};
        case DataType::Type::FLOAT64:
            return {cast.getRawValueAs<val<double>>(), true, isNull};
        default:
            throw QueryCompilerError("HiPy UDF: return type {} is not supported", type);
    }
}
}
}

HiPyPhysicalFunction::HiPyPhysicalFunction(
    const CompiledInlineUdf& compiled, std::vector<PhysicalFunction> arguments, std::vector<DataType> argumentTypes, DataType returnType)
    : program(HiPy::parseProgram(compiled.pipelineBitcode))
    , arguments(std::move(arguments))
    , argumentTypes(std::move(argumentTypes))
    , returnType(std::move(returnType))
{
    const auto& entry = program->functions.at(program->entry);
    if (entry.args.size() != this->argumentTypes.size())
    {
        throw QueryCompilerError("HiPy UDF: {} takes {} arguments, declared {}", program->entry, entry.args.size(), this->argumentTypes.size());
    }
}

VarVal HiPyPhysicalFunction::execute(const Record& record, ArenaRef& arena) const
{
    using nautilus::val;
    std::vector<VarVal> values;
    values.reserve(arguments.size());
    for (nautilus::static_val<size_t> index = 0; index < arguments.size(); ++index)
    {
        values.emplace_back(arguments[index].execute(record, arena));
    }
    /// Whether an argument is NULL, from the null flags of the nullable arguments only (no constant to start from).
    std::optional<val<bool>> anyNull;
    for (nautilus::static_val<size_t> index = 0; index < values.size(); ++index)
    {
        const auto& value = values[index];
        if (value.isNullable())
        {
            anyNull = anyNull.has_value() ? val<bool>(*anyNull || value.isNull()) : value.isNull();
        }
    }

    /// The result type of HiPy's entry function; a nullable result is NULL when the function returns None.
    const auto& entryType = program->functions.at(program->entry).resultType;
    if (entryType.kind == HiPy::Type::Kind::Void)
    {
        /// The function returns None on every path.
        return HiPy::resultValue(HiPy::Interpreter::placeholder(HiPy::Type{.kind = HiPy::Type::Kind::Int, .name = "int", .memberNames = {}, .memberTypes = {}, .inner = {}}), returnType, val<bool>(true));
    }
    const auto& scalarType = entryType.kind == HiPy::Type::Kind::Nullable ? entryType.inner.front() : entryType;
    auto result = HiPy::Interpreter::placeholder(scalarType);
    val<bool> isNull(true);
    const auto callUdf = [&]
    {
        std::vector<HiPy::Value> hipyArguments;
        for (nautilus::static_val<size_t> index = 0; index < values.size(); ++index)
        {
            hipyArguments.push_back(HiPy::argumentValue(values[index], argumentTypes[index]));
        }
        HiPy::Interpreter interpreter(*program, arena);
        const auto returned = interpreter.call(program->entry, std::move(hipyArguments));
        if (const auto* nullable = std::get_if<std::shared_ptr<HiPy::NullableValue>>(&returned))
        {
            HiPy::Interpreter::assign(result, (*nullable)->value);
            isNull = (*nullable)->isNull;
        }
        else
        {
            HiPy::Interpreter::assign(result, returned);
            isNull = val<bool>(false);
        }
    };
    if (!anyNull.has_value())
    {
        callUdf();
    }
    else if (!*anyNull)
    {
        callUdf();
    }
    return HiPy::resultValue(result, returnType, isNull);
}

}
/// NOLINTEND(cppcoreguidelines-pro-bounds-avoid-unchecked-container-access,cppcoreguidelines-pro-type-reinterpret-cast)
