//
// Created by ls on 4/21/25.
//

#pragma once
#include <string_view>
#include <unordered_set>

namespace NES::WellKnown {
    constexpr static std::string_view Constant = "CONSTANT";

    constexpr static std::string_view Integer = "Integer";
    constexpr static std::string_view Bool = "Bool";
    constexpr static std::string_view Float = "Float";
    constexpr static std::string_view Text = "Text";

};
