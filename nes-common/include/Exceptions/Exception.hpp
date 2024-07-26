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

#ifndef NES_COMMON_INCLUDE_EXCEPTIONS_EXCEPTION_HPP_
#define NES_COMMON_INCLUDE_EXCEPTIONS_EXCEPTION_HPP_

#include <source_location>
#include <string>
#include <Util/Logger/Logger.hpp>

namespace NES
{

/**
 * @brief This class is our central class for exceptions.
 * It is used to throw exceptions with a message, a code, a location and a stacktrace.
 * @note do NOT inherit from this class, use the EXCEPTION macro to define exceptions.
 * They should only be defined in <Exceptions/ExceptionDefinitions.hpp>
 */
class Exception final
{
public:
    Exception(std::string message, const uint64_t code, std::source_location loc, std::string trace)
        : message(std::move(message)), errorCode(code), location(loc), stacktrace(std::move(trace))
    {
    }

    std::string& what() noexcept { return message; }
    [[nodiscard]] const std::string& what() const noexcept { return message; }
    [[nodiscard]] uint64_t code() const noexcept { return errorCode; }
    [[nodiscard]] const std::source_location& where() const noexcept { return location; }
    [[nodiscard]] const std::string& stack() const noexcept { return stacktrace; }

private:
    std::string message;
    uint64_t errorCode;
    std::source_location location;
    std::string stacktrace;
};

/**
 * This macro is used to define exceptions of <Exceptions/ExceptionDefinitions.hpp>
 * @param name The name of the exception
 * @param code The code of the exception
 * @param message The message of the exception
 * @return The exception object
 * @note the enum value of the exception can be used to compare with the code of the exception in the catch block
 */
#define EXCEPTION(name, code, message) \
    inline Exception name(const std::source_location& loc = std::source_location::current(), std::string trace = collectStacktrace()) \
    { \
        return Exception(message, code, loc, trace); \
    } \
    namespace ErrorCode \
    { \
    enum \
    { \
        name = code \
    }; \
    }

#include <Exceptions/ExceptionDefinitions.hpp>

/**
 * @brief This function is used to log the current exception.
 * @warning This function should be used only in a catch block.
 */
inline void tryLogCurrentException()
{
    try
    {
        throw;
    }
    catch (const Exception& e)
    {
        NES_ERROR(
            "failed to process with error code ({}) : {}\n {}({}:{}), function `{}`\n",
            e.code(),
            e.what(),
            e.where().file_name(),
            e.where().line(),
            e.where().column(),
            e.where().function_name())
    }
    catch (...)
    {
        NES_ERROR("failed to process with unknown error\n")
    }
}

/**
 * @brief This function is used to get the current exception code.
 * @warning This function should be used only in a catch block.
 */
inline uint64_t getCurrentExceptionCode()
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

} /// Namespace NES

#endif /// NES_COMMON_INCLUDE_EXCEPTIONS_EXCEPTION_HPP_
