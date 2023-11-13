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

#include <Util/Logger/LogSink.hpp>

NES::Logger::LogSink::LogSink(spdlog::source_loc&& loc, std::shared_ptr<spdlog::logger> impl, spdlog::level::level_enum level)
    : impl(std::move(impl)), level(level), loc(std::move(loc)) {}
std::streamsize NES::Logger::LogSink::write(const char* s, std::streamsize n) {
    impl->log(loc, level, "{}", std::string_view(s, n));
    return n;
}