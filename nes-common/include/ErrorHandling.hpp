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

#include <cstdint>
#include <cstdlib>
#include <optional>
#include <ranges>
#include <ostream>
#include <string>
#include <unordered_map>
#include <Util/Ranges.hpp>
#include <string_view>
#include <Util/Logger/Formatter.hpp>
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp> /// NOLINT(misc-include-cleaner) used in macro for NES::Logger::{getInstance(), shutdown()}
#include <cpptrace/cpptrace.hpp>
#include <fmt/core.h>
#include <fmt/format.h>

namespace NES
{

/// Represents error codes as strong type.
/// `ErrorCode` needs to be hidden in `ErrorCodeDetail` namespace to not interfere with the throw function with the same name.
/// We use a trick to keep the shorthand version of error code (e.g., `return ErrorCode::UnknownException;`) via the following using.
namespace ErrorCodeDetail
{
enum ErrorCode
{
#define EXCEPTION(name, code, msg) name = (code),
#include <ExceptionDefinitions.inc>
#undef EXCEPTION
};
}
using ErrorCode = ErrorCodeDetail::ErrorCode;

/// This class is our central class for exceptions. It is used to throw exceptions with a message, a code, a location and a stacktrace.
/// We use cpptrace's lazy stacktrace collection because the eager stacktrace collection became too slow with static project linking.
class Exception final : public cpptrace::lazy_exception
{
public:
    Exception(std::string message, uint64_t code);

    /// copy-constructor is unsaved noexcept because of std::string copy
    Exception(const Exception&) noexcept = default;
    Exception& operator=(const Exception&) noexcept = default;

    std::string& what() noexcept;
    [[nodiscard]] const char* what() const noexcept override;
    [[nodiscard]] ErrorCode code() const noexcept;
    [[nodiscard]] std::optional<const cpptrace::stacktrace_frame> where() const noexcept;

    friend std::ostream& operator<<(std::ostream& os, const Exception& ex);

private:
    std::string message;
    ErrorCode errorCode;
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


/// This macro is used to define exceptions in <ExceptionDefinitions.hpp>
/// @param name The name of the exception
/// @param code The code of the exception
/// @param message The message of the exception
/// @return The exception object
/// @note the enum value of the exception can be used to compare with the code of the exception in the catch block
#define EXCEPTION(name, code, message) \
    inline Exception name() \
    { \
        return Exception(message, code); \
    } \
    inline Exception name(std::string msg) \
    { \
        return Exception(fmt::format("{}; {}\n", message, msg), code); \
    } \
    template <typename... Args> \
    inline Exception name(fmt::format_string<Args...> fmt_msg, Args&&... args) \
    { \
        return Exception(fmt::format("{}; {}\n", message, fmt::format(fmt_msg, std::forward<Args>(args)...)), code); \
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
    namespace ErrorCodes \
    { \
    enum \
    { \
        name = code \
    }; \
    }

#include <ExceptionDefinitions.inc>
#undef EXCEPTION

#ifdef NO_ASSERT
    #define USED_IN_DEBUG [[maybe_unused]]
    #define PRECONDITION(condition, formatString, ...) ((void)0)
    #define INVARIANT(condition, formatString, ...) ((void)0)
#else
    #define USED_IN_DEBUG
    /// Note:
    /// - it is not possible to use positional args since formatString is combined with the fmt string containing {}
    /// - \u001B[0m is the ANSI escape code for "color reset"
    /// - we call NES::Logger::getInstance()->shutdown() to ensure that async logger completely flushes. c.f. https://github.com/gabime/spdlog/wiki/7.-Flush-policy

    /// This documents (and checks) requirements for calling a function. If violated, the function was called incorrectly.
    /// @param condition must be true to correctly call function guarded by precondition
    /// @param formatString can contain `{}` to reference varargs. Must not contain positional reference like `{0}`.
    #define PRECONDITION(condition, formatString, ...) \
        do \
        { \
            if (!(condition)) \
            { \
                auto trace = cpptrace::generate_trace().to_string(true); \
                NES_ERROR("Precondition violated: ({}): " formatString "\u001B[0m\n\n{}", #condition __VA_OPT__(, ) __VA_ARGS__, trace); \
                if (auto logger = NES::Logger::getInstance()) \
                { \
                    logger->shutdown(); \
                } \
                std::terminate(); \
            } \
        } while (false)

    /// This documents what is assumed to be true at this particular point in a program. If violated, there is a misunderstanding and maybe a bug.
    /// @param condition is assumed to be true
    /// @param formatString can contain `{}` to reference varargs. Must not contain positional referencen like `{0}`.
    #define INVARIANT(condition, formatString, ...) \
        do \
        { \
            if (!(condition)) \
            { \
                auto trace = cpptrace::generate_trace().to_string(true); \
                NES_ERROR("Invariant violated: ({}): " formatString "\u001B[0m\n\n{}", #condition __VA_OPT__(, ) __VA_ARGS__, trace); \
                if (auto logger = NES::Logger::getInstance()) \
                { \
                    logger->shutdown(); \
                } \
                std::terminate(); \
            } \
        } while (false)

#endif

/// @brief This function is used to log the current exception.
/// @warning This function should be used only in a catch block.
void tryLogCurrentException();

/// The wrapped exception gets the error code 9999.
Exception wrapExternalException();

/// @brief This function is used to get the current exception code.
/// @warning This function should be used only in a catch block.
[[nodiscard]] ErrorCode getCurrentErrorCode();

[[nodiscard]] std::optional<ErrorCode> errorCodeExists(uint64_t code) noexcept;
[[nodiscard]] std::optional<ErrorCode> errorTypeExists(std::string_view name) noexcept;

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

    | std::ranges::to<std::vector>();
static const std::unordered_map<uint64_t, ExceptionType> EXCEPTIONS_BY_ERROR_CODES
    = std::unordered_map<uint64_t, ExceptionType>{EXCEPTIONS_BY_ERROR_CODES_V.begin(), EXCEPTIONS_BY_ERROR_CODES_V.end()};

[[maybe_unused]] static ExceptionType typeFromCode(uint64_t code)
{
    auto found = EXCEPTIONS_BY_ERROR_CODES.find(code);
    if (found == EXCEPTIONS_BY_ERROR_CODES.end())
    {
        return EXCEPTIONS_BY_ERROR_CODES.at(ErrorCodes::UnknownException);
    }
    return found->second;
}
}

namespace fmt
{
FMT_FORMAT_AS(NES::ErrorCodeDetail::ErrorCode, std::uint64_t);
FMT_OSTREAM(NES::Exception);
}
