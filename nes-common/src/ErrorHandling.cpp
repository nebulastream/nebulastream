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

#include <Util/Logger/Logger.hpp>
#include <fmt/format.h>
#include <ErrorHandling.hpp>

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

Exception::Exception(std::string message, const uint64_t code) : message(std::move(message)), errorCode(code)
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

uint64_t Exception::code() const noexcept
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

std::string formatLogMessage(const Exception& e)
{
    if (e.where().has_value())
    {
        auto exceptionLocation = e.where().value();
        if constexpr (logWithStacktrace)
        {
            constexpr auto ANSI_COLOR_RESET = "\u001B[0m";
            return fmt::format(
                "failed to process with error code ({}) : {}\n{}:{}:{} in function `{}`\n\n{}\n",
                e.code(),
                e.what(),
                exceptionLocation.filename,
                exceptionLocation.line,
                exceptionLocation.column,
                exceptionLocation.symbol,
                ANSI_COLOR_RESET + e.trace().to_string(true));
        }
        return fmt::format(
            "failed to process with error code ({}) : {}\n{}:{}:{} in function `{}`\n",
            e.code(),
            e.what(),
            exceptionLocation.filename,
            exceptionLocation.line,
            exceptionLocation.column,
            exceptionLocation.symbol);
    }
    return fmt::format("failed to process with error code ({}) : {}\n\n{}\n", e.code(), e.what(), e.trace().to_string());
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
        NES_ERROR("failed to process with error : {}\n", e.what())
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


uint64_t getCurrentExceptionCode()
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
