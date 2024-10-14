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
#include <ErrorHandling.hpp>

namespace NES
{

#ifdef NES_LOG_WITH_STACKTRACE
constexpr bool logWithStacktrace = true;
#else
constexpr bool logWithStacktrace = false;
#endif

Exception::Exception(std::string message, const uint64_t code, std::source_location loc, std::string trace)
    : message(std::move(message)), errorCode(code), location(loc), stacktrace(std::move(trace))
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

const std::source_location& Exception::where() const noexcept
{
    return location;
}

const std::string& Exception::stack() const noexcept
{
    return stacktrace;
}


std::string formatLogMessage(const Exception& e)
{
    if constexpr (logWithStacktrace)
    {
        return fmt::format(
            "failed to process with error code ({}) : {}\n{}:{}:{} in function `{}`\n\n{}\n",
            e.code(),
            e.what(),
            e.where().file_name(),
            e.where().line(),
            e.where().column(),
            e.where().function_name(),
            e.stack());
    }
    else
    {
        return fmt::format(
            "failed to process with error code ({}) : {}\n{}:{}:{} in function `{}`\n",
            e.code(),
            e.what(),
            e.where().file_name(),
            e.where().line(),
            e.where().column(),
            e.where().function_name());
    }
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
