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
#include <string_view>

#include <rust/cxx.h>

struct SinkContext
{
    std::function<void(std::string_view)> onError;
    std::function<void(size_t)> onFlush;
};

void on_error(const SinkContext& context, rust::String message);

void on_flush(const SinkContext& context, size_t epoch);
