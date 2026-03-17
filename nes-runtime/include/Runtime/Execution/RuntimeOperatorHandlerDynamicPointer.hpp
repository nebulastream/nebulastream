#pragma once

#include <string>
#include <string_view>
#include <Runtime/Execution/OperatorHandler.hpp>

namespace NES
{
constexpr std::string_view kRuntimeOperatorHandlerDynamicPointerPrefix = "operator_handler:";

inline std::string createRuntimeOperatorHandlerName(const OperatorHandlerId operatorHandlerId)
{
    return std::string(kRuntimeOperatorHandlerDynamicPointerPrefix) + std::to_string(operatorHandlerId.getRawValue());
}

}
