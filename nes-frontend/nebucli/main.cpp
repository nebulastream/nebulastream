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

#include <Plugins/BuiltinPlugins.hpp>
#include <Version.hpp>

extern "C" int nebucli_main();

int main(int argc, char** argv)
{
    /// Answered here rather than in the Rust parser, which requires a subcommand and so never sees a bare -v.
    if (NES::hasVersionFlag(argc, argv))
    {
        NES::printVersion("nes-cli");
        return 0;
    }
    /// Registries are populated at runtime rather than at configure time, so nothing this binary links is reachable
    /// until they are loaded. Done here rather than in Rust, because the registries are C++.
    NES::loadBuiltinPlugins();
    return nebucli_main();
}
