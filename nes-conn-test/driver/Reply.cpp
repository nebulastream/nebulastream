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

#include <Reply.hpp>

#include <cstdint>
#include <string>
#include <string_view>

#include <ErrorHandling.hpp>

#include <yyjson.h>
#include <rfl/json/write.hpp>

namespace NES::ConnTest
{
namespace
{
/// The member order is the key order of the reply.
struct OkReply
{
    bool ok = true;
    std::string phase;
};

struct FillReply
{
    bool ok = true;
    std::string phase;
    bool eos = false;
};

struct ErrorReply
{
    bool ok = false;
    std::string phase;
    std::string origin;
    std::uint16_t code = 0;
    std::string name;
    std::string message;
};

template <typename Reply>
std::string write(const Reply& reply)
{
    /// Exception messages can contain any bytes, for example from a file path.
    /// Escaping everything outside ASCII keeps the line valid JSON and replaces invalid UTF-8 with U+FFFD.
    return rfl::json::write(reply, YYJSON_WRITE_ESCAPE_UNICODE | YYJSON_WRITE_ALLOW_INVALID_UNICODE);
}
}

/// Not magic_enum: the error codes span 1000 to 10000, and an enum range that wide makes clang-tidy take about 28 minutes on this file.
/// The names match the ones magic_enum would return.
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

std::string errorReply(const ErrorInfo& err)
{
    return write(ErrorReply{.phase = err.phase, .origin = err.origin, .code = err.code, .name = err.name, .message = err.message});
}

std::string okReply(const std::string_view phase)
{
    return write(OkReply{.phase = std::string{phase}});
}

std::string fillReply(const std::string_view phase, const bool eos)
{
    return write(FillReply{.phase = std::string{phase}, .eos = eos});
}

}
