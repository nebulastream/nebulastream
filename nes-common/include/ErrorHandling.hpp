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

#include <cstdlib>
#include <iostream>
#include <optional>
#include <ranges>
#include <string>
#include <unordered_map>
#include <Util/Ranges.hpp>
#include <cpptrace/cpptrace.hpp>
#include <fmt/core.h>

namespace NES
{
/// This class is our central class for exceptions. It is used to throw exceptions with a message, a code, a location and a stacktrace.
/// We use cpptrace's lazy stacktrace collection because the eager stacktrace collection became too slow with static project linking.
class Exception final : public cpptrace::lazy_exception
{
public:
    Exception(std::string message, uint64_t code);

    std::string& what() noexcept;
    [[nodiscard]] const char* what() const noexcept override;
    [[nodiscard]] uint64_t code() const noexcept;
    [[nodiscard]] std::optional<const cpptrace::stacktrace_frame> where() const noexcept;

private:
    std::string message;
    uint64_t errorCode;
};

template <uint64_t errorCode>
class ExceptionRegistration
{
    uint64_t code;
public:
    static constexpr std::string_view exceptionName = "";
    static constexpr std::string_view errorMessage = "";
    //constexpr ExceptionRegistration() noexcept;
    static constexpr uint64_t getErrorCode() { return errorCode; }
    static constexpr std::string getMessage();
    static constexpr std::string getName();
};

class ExceptionType
{
    uint64_t code;
    std::string_view name;
    std::string_view message;

public:
    constexpr ExceptionType() : code(0), name(""), message("") { }
    constexpr ExceptionType(const uint64_t code, const std::string_view name, const std::string_view message)
        : code(code), name(name), message(message)
    {
    }

    constexpr ExceptionType(const ExceptionType& other) = default;
    constexpr ExceptionType(ExceptionType&& other) noexcept = default;
    constexpr ExceptionType& operator=(const ExceptionType& other) = default;
    constexpr ExceptionType& operator=(ExceptionType&& other) noexcept = default;

    inline Exception create() const { return Exception{std::string{message}, code}; }
    inline Exception create(const std::string& msg) const { return Exception{std::string(message) + "; " + msg, code}; }
    template <typename... Args>
    inline Exception create(fmt::format_string<Args...> fmt_msg, Args&&... args)
    {
        return Exception(fmt::format("{}; {}", message, fmt::format(fmt_msg, std::forward<Args>(args)...)), code);
    }

    [[nodiscard]] constexpr uint64_t getErrorCode() const { return code; }
    [[nodiscard]] constexpr std::string_view getName() const { return name; }
    [[nodiscard]] constexpr std::string_view getErrorMessage() const { return message; }
};


struct ErrorCodeDefined
{
private:
    template <uint64_t errorCode, class Dummy = decltype(ExceptionRegistration<errorCode>{})>
    static constexpr bool exist(int)
    {
        return true;
    }

    template <uint64_t errorCode>
    static constexpr bool exist(char)
    {
        return false;
    }

public:
    template <uint64_t errorCode>
    static constexpr bool check()
    {
        return exist<errorCode>(69);
    }
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
    inline Exception name() \
    { \
        return Exception(message, code); \
    } \
    inline Exception name(std::string msg) \
    { \
        return Exception(std::string(message) + "; " + msg, code); \
    } \
    template <typename... Args> \
    inline Exception name(fmt::format_string<Args...> fmt_msg, Args&&... args) \
    { \
        return Exception(fmt::format("{}; {}", message, fmt::format(fmt_msg, std::forward<Args>(args)...)), code); \
    } \
    template <> \
    class ExceptionRegistration<code> \
    { \
    public: \
        static constexpr std::string_view exceptionName = #name; \
        static constexpr std::string_view errorMessage = message; \
        constexpr ExceptionRegistration<code>() \
        { \
        } \
        static constexpr std::string getName() \
        { \
            return #name; \
        } \
        static constexpr std::string getMessage() \
        { \
            return message; \
        } \
    }; \
    namespace ErrorCode \
    { \
    enum \
    { \
        name = code \
    }; \
    }

#include <ExceptionDefinitions.inc>
#undef EXCEPTION

/**
 * A precondition is a condition that must be true at the beginning of a function. If a precondition got violated, this usually means that
 * the caller of the functions made an error.
 * @param condition The condition that should be true
 * @param message The message that should be printed if the condition is false
 */
#define PRECONDITION(condition, formatString, ...) \
    do \
    { \
        if (!(condition)) \
        { \
            std::cerr << "Precondition violated: (" #condition ") at " << __FILE__ << ":" << __LINE__ << " : " \
                      << fmt::format(fmt::runtime(formatString) __VA_OPT__(, ) __VA_ARGS__) << "\n"; \
            cpptrace::generate_trace().print(); \
            std::terminate(); \
        } \
    } while (false)

/**
 * @brief An invariant is a condition that is always true at a particular point in a program. If an invariant gets violated, this usually
 * means that there is a bug in the program.
 * @param condition The condition that should be true
 * @param message The message that should be printed if the condition is false
 */
#define INVARIANT(condition, formatString, ...) \
    do \
    { \
        if (!(condition)) \
        { \
            std::cerr << "Invariant violated: (" #condition ") at " << __FILE__ << ":" << __LINE__ << " : " \
                      << fmt::format(fmt::runtime(formatString) __VA_OPT__(, ) __VA_ARGS__) << "\n"; \
            cpptrace::generate_trace().print(); \
            std::terminate(); \
        } \
    } while (false)

/**
 * @brief This function is used to log the current exception.
 * @warning This function should be used only in a catch block.
 */
void tryLogCurrentException();

/// The wrapped exception gets the error code 9999.
Exception wrapExternalException();

/**
 * @brief This function is used to get the current exception code.
 * @warning This function should be used only in a catch block.
 */
[[nodiscard]] uint64_t getCurrentExceptionCode();

template <auto start, auto end, class F>
constexpr void constexprFor(F&& f)
{
    if constexpr (start < end)
    {
        f(std::integral_constant<decltype(start), start>());
        constexprFor<start + 1, end>(f);
    }
}

template <uint64_t rangeEnd>
constexpr std::array<ExceptionType, rangeEnd> findExceptionRegistrations()
{
    std::vector<ExceptionType> registrations{};

    [&]<uint64_t... errorCode>(std::integer_sequence<uint64_t, errorCode...>)
    {
        (
            [&]
            {
                if (ErrorCodeDefined::check<errorCode>())
                {
                    const std::string_view name = ExceptionRegistration<errorCode>::exceptionName;
                    const std::string_view message = ExceptionRegistration<errorCode>::errorMessage;
                    const uint64_t code = errorCode;
                    registrations.emplace_back(code, name, message);
                }
            }(),
            ...);
    }(std::make_integer_sequence<uint64_t, rangeEnd>());

    std::array<ExceptionType, rangeEnd> registrationArray;
    std::copy(registrations.begin(), registrations.end(), registrationArray.begin());
    return registrationArray;
}

constexpr auto exceptions = findExceptionRegistrations<10000>();
static const std::vector<std::pair<uint64_t, ExceptionType>> EXCEPTIONS_BY_ERROR_CODES_V
    = exceptions | std::ranges::views::filter([](auto exception) { return exception.getErrorCode() != 0; })
    | std::ranges::views::transform(
          [](auto exceptionType)
          { return std::pair<uint64_t, ExceptionType>{static_cast<uint64_t>(exceptionType.getErrorCode()), exceptionType}; })

    | ranges::to<std::vector>();
static const std::unordered_map<uint64_t, ExceptionType> EXCEPTIONS_BY_ERROR_CODES
    = std::unordered_map<uint64_t, ExceptionType>{EXCEPTIONS_BY_ERROR_CODES_V.begin(), EXCEPTIONS_BY_ERROR_CODES_V.end()};

namespace ErrorCode
{
[[maybe_unused]] static ExceptionType typeFromCode(uint64_t code)
{
    auto found = EXCEPTIONS_BY_ERROR_CODES.find(code);
    if (found == EXCEPTIONS_BY_ERROR_CODES.end())
    {
        return EXCEPTIONS_BY_ERROR_CODES.at(UnknownException);
    }
    return found->second;
}
}

}