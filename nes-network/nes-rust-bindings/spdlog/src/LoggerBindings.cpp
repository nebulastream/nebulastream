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

#include <LoggerBindings.hpp>

#include <cmath>
#include <memory>
#include <string>
#include <string_view>
#include <unordered_map>
#include <magic_enum/magic_enum.hpp>
#include <rust/cxx.h>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <spdlog/mdc.h>

namespace
{

/// Logger has to translate from rust strings to c++ strings for every filename. Because file names don't change, this can be cached.
thread_local std::unordered_map<const char*, std::string> filenames = {}; ///NOLINT(cppcoreguidelines-avoid-non-const-global-variables)
}

void log(
    const std::shared_ptr<spdlog::logger>& logger,
    int level,
    rust::cxxbridge1::Str filename,
    unsigned int line,
    rust::cxxbridge1::Str message)
{
    const auto* filenameCString = filenames.try_emplace(filename.data(), filename.begin(), filename.end()).first->second.c_str();
    logger->log(
        spdlog::source_loc{filenameCString, static_cast<int>(line), "RUST"},
        magic_enum::enum_cast<spdlog::level::level_enum>(level).value_or(spdlog::level::level_enum::off),
        std::string_view{message});
}

void mdc_insert(rust::Str key, rust::Str value)
{
    spdlog::mdc::put(static_cast<std::string>(key), static_cast<std::string>(value));
}

void mdc_remove(rust::Str key)
{
    spdlog::mdc::remove(static_cast<std::string>(key));
}
