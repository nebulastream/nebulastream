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

/// Builds the configuration of one invocation from the command line, the disable config file and the referenced yaml files.
/// Exits the process on a malformed command line, and on the meta commands that only print (help, and the list of discovered tests).
[[nodiscard]] Config parseConfig(int argc, const char** argv);

}
