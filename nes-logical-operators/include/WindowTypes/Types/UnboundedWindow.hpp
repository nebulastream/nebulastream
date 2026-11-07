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

#include <cstddef>
#include <functional>
#include <ostream>
#include <Util/Logger/Formatter.hpp>

namespace NES::Windowing
{
/// A window that never closes: every record is joined with every record of the other input that ever arrives.
/// Only valid for joins. It has no size or slide; the operator emits results incrementally as event time advances.
struct UnboundedWindow
{
    bool operator==(const UnboundedWindow&) const = default;

    friend std::ostream& operator<<(std::ostream& os, const UnboundedWindow&) { return os << "UnboundedWindow"; }
};
}

template <>
struct std::hash<NES::Windowing::UnboundedWindow>
{
    std::size_t operator()(const NES::Windowing::UnboundedWindow&) const noexcept { return 0; }
};

FMT_OSTREAM(NES::Windowing::UnboundedWindow);
