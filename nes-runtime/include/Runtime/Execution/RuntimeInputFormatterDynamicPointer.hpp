#pragma once

#include <cstdint>
#include <string>
#include <string_view>

namespace NES
{
constexpr std::string_view kRuntimeInputFormatterDynamicPointerPrefix = "runtime_input_formatter:";

inline std::string createRuntimeInputFormatterName(const uint64_t runtimeInputFormatterSlot)
{
    return std::string(kRuntimeInputFormatterDynamicPointerPrefix) + std::to_string(runtimeInputFormatterSlot);
}

}
