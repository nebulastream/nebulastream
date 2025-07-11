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
#include <ErrorHandling.hpp>

#include <cstddef>
#include <cstdint>
#include <exception>
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <Util/Logger/Logger.hpp>
#include <cpptrace/basic.hpp>
#include <fmt/base.h>
#include <fmt/format.h>
#include <magic_enum/magic_enum.hpp>
#include <from_current.hpp>

/// formater for cpptrace::nullable
namespace fmt
{
template <>
struct formatter<cpptrace::nullable<unsigned int>>
{
    static constexpr auto parse(format_parse_context& ctx) -> decltype(ctx.begin()) { return ctx.end(); }

    template <typename FormatContext>
    auto format(const cpptrace::nullable<unsigned int>& nullable, FormatContext& ctx) const -> decltype(ctx.out())
    {
        if (nullable.has_value())
        {
            return format_to(ctx.out(), "{}", nullable.value());
        }
        return format_to(ctx.out(), "unknown");
    }
};
}

namespace NES
{

#ifdef NES_LOG_WITH_STACKTRACE
constexpr bool logWithStacktrace = true;
#else
constexpr bool logWithStacktrace = false;
#endif

Exception::Exception(std::string message, const uint64_t code) : message(std::move(message)), errorCode(static_cast<ErrorCode>(code))
{
}

std::string& Exception::what() noexcept
{
    return message;
}

const char* Exception::what() const noexcept
{
    return message.c_str();
}

ErrorCode Exception::code() const noexcept
{
    return errorCode;
}

std::optional<const cpptrace::stacktrace_frame> Exception::where() const noexcept
{
    constexpr size_t exceptionFrame = 2; /// we need to go two frames back to the exception source

    if (this->trace().frames.size() > exceptionFrame)
    {
        return this->trace().frames[exceptionFrame];
    }

    /// This cases should never happen
    return std::nullopt;
}

std::ostream& operator<<(std::ostream& os, const Exception& e)
{
    os << fmt::format("({}): {}", e.code(), e.what());
    return os;
}

constexpr std::string_view ansiColorReset = "\u001B[0m";

std::string formatLogMessage(const Exception& e)
{
    if constexpr (not logWithStacktrace)
    {
        return fmt::format("failed to process with error code ({}) : {}\n", e.code(), e.what());
    }

    if (!e.where())
    {
        return fmt::format(
            "failed to process with error code ({}) : {}\n\n{}{}\n", e.code(), e.what(), ansiColorReset, e.trace().to_string(true));
    }

    auto exceptionLocation = e.where().value();
    return fmt::format(
        "failed to process with error code ({}) : {}\n{}:{}:{} in function `{}`\n\n{}{}\n",
        e.code(),
        e.what(),
        exceptionLocation.filename,
        exceptionLocation.line,
        exceptionLocation.column,
        exceptionLocation.symbol,
        ansiColorReset,
        e.trace().to_string(true));
}

std::string formatLogMessage(const std::exception& e)
{
    if constexpr (not logWithStacktrace)
    {
        return fmt::format("failed to process with error : {}", e.what());
    }
    constexpr auto ansiColorReset = "\u001B[0m";
    const auto& trace = cpptrace::from_current_exception().to_string(true);
    return fmt::format("failed to process with error : {}\n{}{}", e.what(), ansiColorReset, trace);
}

void tryLogCurrentException()
{
    try
    {
        throw;
    }
    catch (const Exception& e)
    {
        NES_ERROR("{}", formatLogMessage(e))
    }
    catch (const std::exception& e)
    {
        NES_ERROR("{}", formatLogMessage(e));
    }
    catch (...)
    {
        NES_ERROR("failed to process with unknown error\n")
    }
}
Exception wrapExternalException()
{
    try
    {
        throw;
    }
    catch (const Exception& e)
    {
        return e;
    }
    catch (const std::exception& e)
    {
        return UnknownException(e.what());
    }
    catch (...)
    {
        return UnknownException();
    }
}


ErrorCode getCurrentErrorCode()
{
    try
    {
        throw;
    }
    catch (const Exception& e)
    {
        return e.code();
    }
    catch (...)
    {
        return ErrorCode::UnknownException;
    }
}

}

/// errorCodeExists() and errorTypeExists() use internally magic_enum. To work with our generated error code enum we have to define
/// min and max in advance
namespace magic_enum::customize
{
template <>
struct enum_range<NES::ErrorCode>
{
    static constexpr int min = 1000;
    static constexpr int max = 10000;
};
}

namespace NES
{

std::optional<ErrorCode> errorCodeExists(uint64_t code) noexcept
{
    return magic_enum::enum_cast<ErrorCode>(code);
}

std::optional<ErrorCode> errorTypeExists(std::string_view codeOrTypeStr) noexcept
{
    return magic_enum::enum_cast<ErrorCode>(codeOrTypeStr);
}
}
