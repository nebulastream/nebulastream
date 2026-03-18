// Minimal nlohmann/json stub for WASM build.
// Only provides enough API surface for QueryId.hpp adl_serializer to compile.
#pragma once

#include <string>
#include <stdexcept>

namespace nlohmann
{

class json
{
public:
    json() = default;

    bool is_object() const { return false; }

    json at(const std::string&) const { return {}; }

    template <typename T>
    T get() const { return T{}; }

    json& operator[](const std::string&) { return *this; }
    const json& operator[](const std::string&) const { return *this; }

    json& operator=(const std::string&) { return *this; }
};

template <typename T>
struct adl_serializer
{
    static T from_json(const json&) { return T{}; }
    static void to_json(json&, const T&) {}
};

} // namespace nlohmann
