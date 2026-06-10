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

#include <Report.hpp>

#include <cstdint>
#include <string>
#include <string_view>

#include <ErrorHandling.hpp>

#include <fmt/format.h>

/// `c` (current char), `i` (digest byte index), `len` and `out` are the canonical
/// scanner / hex-encoder loop names; banding the file keeps them.
namespace NES::ConnTest
{
namespace
{
/// Map an ErrorCode to its enumerator name via the X-macro the enum is itself generated from.
/// This deliberately avoids `magic_enum::enum_name(ErrorCode)`: ErrorCode is sparse over
/// [1000, 10000], so magic_enum needs an enum_range that wide, and scanning ~9000 candidate
/// values instantiates a giant AST that makes clang-tidy's whole-AST checks (misc-const-correctness,
/// readability-magic-numbers, pro-bounds-array-to-pointer-decay) take ~28 min on this one TU.
/// The switch is O(number of codes) and the names are identical to magic_enum's.
std::string_view errorCodeName(const ErrorCode code)
{
    switch (code)
    {
#define EXCEPTION(name, codeValue, message) \
    case static_cast<ErrorCode>(codeValue): \
        return #name;
#include <ExceptionDefinitions.inc>
#undef EXCEPTION
    }
    return {};
}
}

std::string jsonEscape(const std::string_view input)
{
    std::string out;
    out.reserve(input.size() + 2);
    for (const char controlChar : input)
    {
        switch (controlChar)
        {
            case '"':
                out += "\\\"";
                break;
            case '\\':
                out += "\\\\";
                break;
            case '\b':
                out += "\\b";
                break;
            case '\f':
                out += "\\f";
                break;
            case '\n':
                out += "\\n";
                break;
            case '\r':
                out += "\\r";
                break;
            case '\t':
                out += "\\t";
                break;
            default:
                if (constexpr unsigned char asciiPrintableMin = 0x20; static_cast<unsigned char>(controlChar) < asciiPrintableMin)
                {
                    out += fmt::format("\\u{:04x}", static_cast<unsigned>(controlChar));
                }
                else
                {
                    out += controlChar;
                }
        }
    }
    return out;
}

std::string errorReplyLine(const ErrorInfo& err)
{
    return fmt::format(
        R"({{"ok":false,"phase":"{}","origin":"{}","code":{},"name":"{}","message":"{}"}})",
        jsonEscape(err.phase),
        jsonEscape(err.origin),
        err.code,
        jsonEscape(err.name),
        jsonEscape(err.message));
}

ErrorInfo connectorErrorInfo(const std::string_view phase, const Exception& exception)
{
    return ErrorInfo{
        .origin = "connector",
        .phase = std::string{phase},
        .name = std::string{errorCodeName(exception.code())},
        .message = exception.what(),
        .code = static_cast<std::uint16_t>(exception.code())};
}

ErrorInfo harnessErrorInfo(const std::string_view phase, const std::string_view name, const std::string_view msg)
{
    return ErrorInfo{.origin = "harness", .phase = std::string{phase}, .name = std::string{name}, .message = std::string{msg}, .code = 0};
}

void captureConnector(Report& report, const std::string_view phase, const Exception& exception)
{
    report.error = connectorErrorInfo(phase, exception);
}

void captureHarness(Report& report, const std::string_view phase, const std::string_view name, const std::string_view msg)
{
    report.error = harnessErrorInfo(phase, name, msg);
}

}
