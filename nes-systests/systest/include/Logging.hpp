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

#include <Config/Config.hpp>

namespace NES
{

/// Points the logger at this run's log file, prints that path, and updates the `latest.log` symlink next to it.
/// The configuration supplies the path.
/// When it supplies none, the path includes the process id and goes under the build directory, so two concurrent runs write to
/// different files.
void setupLogging(const Config& config);

}
