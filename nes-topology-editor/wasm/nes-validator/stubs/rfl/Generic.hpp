#pragma once
// Minimal rfl::Generic stub for WASM build -- reflectcpp not available for Emscripten
#include <string>
#include <variant>
#include <vector>
#include <map>
#include <cstdint>
#include <cstdlib>
#include <type_traits>

namespace rfl {

class Generic {
public:
    Generic() = default;
    // Accept any type without constraining constructibility
    template<typename T, typename = std::enable_if_t<!std::is_same_v<std::decay_t<T>, Generic>>>
    Generic(T&&) {}
    bool is_null() const { return true; }
};

template<typename T>
Generic to_generic(const T&) { return Generic{}; }

template<typename T>
struct Result {
    bool has_value() const { return false; }
    [[noreturn]] T value() const { std::abort(); }
};

template<typename T>
Result<T> from_generic(const Generic&) { return Result<T>{}; }

template<typename T>
struct Reflector {};

} // namespace rfl
