/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#include <Windowing/Runtime/WindowState.hpp>

namespace NES::Windowing {

WindowState::WindowState(uint64_t start, uint64_t end) : start(start), end(end) {}

uint64_t WindowState::getStartTs() const { return start; }
uint64_t WindowState::getEndTs() const { return end; }

}// namespace NES::Windowing