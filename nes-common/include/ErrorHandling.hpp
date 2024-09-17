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

#pragma once

#include <source_location>
#include <sstream>
#include <string>
#include <Util/StacktraceLoader.hpp>
#include <cpptrace/cpptrace.hpp>

namespace NES
{

/**
 * This class is our central class for exceptions. It is used to throw exceptions with a message, a code, a location and a stacktrace.
 * @note do NOT inherit from this class, use the EXCEPTION macro to define exceptions. They should only be defined in
 * <ExceptionDefinitions.hpp>
 */
class Exception final : public cpptrace::lazy_exception
{
public:
    Exception(std::string message, const uint64_t code, std::source_location loc);

    std::string& what() noexcept;
    [[nodiscard]] const char* what() const noexcept override;
    [[nodiscard]] uint64_t code() const noexcept;
    [[nodiscard]] const std::source_location& where() const noexcept;

private:
    std::string message;
    uint64_t errorCode;
    std::source_location location;
};

/**
 * This macro is used to define exceptions in <ExceptionDefinitions.hpp>
 * @param name The name of the exception
 * @param code The code of the exception
 * @param message The message of the exception
 * @return The exception object
 * @note the enum value of the exception can be used to compare with the code of the exception in the catch block
 */
#define EXCEPTION(name, code, message) \
    inline Exception name(const std::source_location& loc = std::source_location::current()) \
    { \
        return Exception(message, code, loc); \
    } \
    inline Exception name(std::string msg, const std::source_location& loc = std::source_location::current()) \
    { \
        return Exception(std::string(message) + "; " + msg, code, loc); \
    } \
    namespace ErrorCode \
    { \
    enum \
    { \
        name = code \
    }; \
    }

#include <ExceptionDefinitions.hpp>
#undef EXCEPTION

/**
 * A precondition is a condition that must be true at the beginning of a function. If a precondition got violated, this usually means that
 * the caller of the functions made an error.
 * @param condition The condition that should be true
 * @param message The message that should be printed if the condition is false
 */
#define PRECONDITION(condition, message) \
    { \
        std::stringstream messageStream; \
        messageStream << message; \
        if (!(condition)) \
        { \
            throw PreconditionViolated(messageStream.str()); \
        } \
    }

/**
 * @brief An invariant is a condition that is always true at a particular point in a program. If an invariant gets violated, this usually
 * means that there is a bug in the program.
 * @param condition The condition that should be true
 * @param message The message that should be printed if the condition is false
 */
#define INVARIANT(condition, message) \
    { \
        std::stringstream messageStream; \
        messageStream << message; \
        if (!(condition)) \
        { \
            throw InvariantViolated(messageStream.str()); \
        } \
    }

/**
 * @brief This function is used to log the current exception.
 * @warning This function should be used only in a catch block.
 */
void tryLogCurrentException();

/**
 * @brief This function is used to get the current exception code.
 * @warning This function should be used only in a catch block.
 */
[[nodiscard]] uint64_t getCurrentExceptionCode();

}
