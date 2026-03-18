/*
    Stub replacement for cpptrace/exceptions.hpp.
    Provides lazy_exception as a plain std::exception subclass so that
    NES::Exception (which inherits from cpptrace::lazy_exception) compiles
    without the real cpptrace library.
*/
#ifndef CPPTRACE_EXCEPTIONS_HPP
#define CPPTRACE_EXCEPTIONS_HPP

#include <cpptrace/basic.hpp>

#include <exception>
#include <string>
#include <utility>

namespace cpptrace {

    namespace detail {
        class lazy_trace_holder {
            bool resolved = false;
        public:
            lazy_trace_holder() = default;
            explicit lazy_trace_holder(raw_trace&&) {}
            explicit lazy_trace_holder(stacktrace&&) : resolved(true) {}
            const raw_trace& get_raw_trace() const { static raw_trace t; return t; }
            stacktrace& get_resolved_trace() { static stacktrace s; return s; }
            const stacktrace& get_resolved_trace() const { static const stacktrace s; return s; }
        };

        inline raw_trace get_raw_trace_and_absorb(std::size_t = 0, std::size_t = 0) { return {}; }
    }

    class exception : public std::exception {
    public:
        const char* what() const noexcept override = 0;
        virtual const char* message() const noexcept = 0;
        virtual const stacktrace& trace() const noexcept = 0;
    };

    class lazy_exception : public exception {
        mutable detail::lazy_trace_holder trace_holder;
        mutable std::string what_string;

    public:
        explicit lazy_exception(
            raw_trace&& trace = detail::get_raw_trace_and_absorb()
        ) : trace_holder(std::move(trace)) {}

        const char* what() const noexcept override { return what_string.c_str(); }
        const char* message() const noexcept override { return what_string.c_str(); }
        const stacktrace& trace() const noexcept override {
            static const stacktrace empty;
            return empty;
        }
    };

    class exception_with_message : public lazy_exception {
        mutable std::string user_message;
    public:
        explicit exception_with_message(
            std::string&& message_arg,
            raw_trace&& trace = detail::get_raw_trace_and_absorb()
        ) noexcept : lazy_exception(std::move(trace)), user_message(std::move(message_arg)) {}

        const char* message() const noexcept override { return user_message.c_str(); }
    };

    [[noreturn]] inline void rethrow_and_wrap_if_needed(std::size_t = 0) { throw; }
}

#define CPPTRACE_WRAP_BLOCK(statements) do { \
        try { \
            statements \
        } catch(...) { \
            ::cpptrace::rethrow_and_wrap_if_needed(); \
        } \
    } while(0)

#define CPPTRACE_WRAP(expression) [&] () -> decltype((expression)) { \
        try { \
            return expression; \
        } catch(...) { \
            ::cpptrace::rethrow_and_wrap_if_needed(1); \
        } \
    } ()

#endif
