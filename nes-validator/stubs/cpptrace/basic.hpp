/*
    Stub replacement for cpptrace/basic.hpp.
    Provides minimal raw_trace, stacktrace_frame, and stacktrace so that
    ErrorHandling.hpp and Exception class compile without the real cpptrace library.
*/
#ifndef CPPTRACE_BASIC_HPP
#define CPPTRACE_BASIC_HPP

#include <cpptrace/forward.hpp>

#include <cstdint>
#include <limits>
#include <string>
#include <vector>
#include <iosfwd>

#define CPPTRACE_EXPORT
#define CPPTRACE_FORCE_NO_INLINE

namespace cpptrace {

    template <typename T>
    struct nullable {
        T raw_value;
        constexpr bool has_value() const noexcept { return raw_value != (std::numeric_limits<T>::max)(); }
        constexpr T value() const noexcept { return raw_value; }
        constexpr bool operator==(const nullable& other) const noexcept { return raw_value == other.raw_value; }
        constexpr bool operator!=(const nullable& other) const noexcept { return raw_value != other.raw_value; }
        constexpr static T null_value() noexcept { return (std::numeric_limits<T>::max)(); }
        constexpr static nullable null() noexcept { return { null_value() }; }
    };

    struct stacktrace_frame {
        frame_ptr raw_address = 0;
        frame_ptr object_address = 0;
        nullable<std::uint32_t> line{0};
        nullable<std::uint32_t> column{0};
        std::string filename;
        std::string symbol;
        bool is_inline = false;

        bool operator==(const stacktrace_frame&) const { return true; }
        bool operator!=(const stacktrace_frame&) const { return false; }
        std::string to_string() const { return ""; }
        std::string to_string(bool) const { return ""; }
    };

    struct stacktrace {
        std::vector<stacktrace_frame> frames;
        static stacktrace current(std::size_t = 0) { return {}; }
        static stacktrace current(std::size_t, std::size_t) { return {}; }
        void print() const {}
        void print(std::ostream&) const {}
        void print(std::ostream&, bool) const {}
        void clear() { frames.clear(); }
        bool empty() const noexcept { return true; }
        std::string to_string(bool = false) const { return ""; }
    };

    struct raw_trace {
        std::vector<frame_ptr> frames;
        static raw_trace current(std::size_t = 0) { return {}; }
        static raw_trace current(std::size_t, std::size_t) { return {}; }
        stacktrace resolve() const { return {}; }
        void clear() { frames.clear(); }
        bool empty() const noexcept { return true; }
    };

    struct object_frame {
        frame_ptr raw_address = 0;
        frame_ptr object_address = 0;
        std::string object_path;
    };

    struct object_trace {
        std::vector<object_frame> frames;
        static object_trace current(std::size_t = 0) { return {}; }
        void clear() { frames.clear(); }
        bool empty() const noexcept { return true; }
    };

    inline raw_trace generate_raw_trace(std::size_t = 0) { return {}; }
    inline raw_trace generate_raw_trace(std::size_t, std::size_t) { return {}; }
    inline stacktrace generate_trace(std::size_t = 0) { return {}; }
    inline stacktrace generate_trace(std::size_t, std::size_t) { return {}; }
}

#endif
