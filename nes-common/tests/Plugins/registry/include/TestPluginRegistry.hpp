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

#include <functional>
#include <string>
#include <Util/RuntimeRegistry.hpp>

namespace NES
{

/// Test-only entry: returns the key it was registered under.
using TestPluginEntry = std::function<std::string()>;

/// Dummy registry for exercising the plugin mechanism (see PluginCatalogTest.cpp): owned by the
/// test host executable, announced through the built-in collection, and filled by the generated
/// nes-test-plugin module. Test-only, so the base's inline instance() suffices — host and plugin
/// share one binary+libnes world and the plugin never references the singleton anyway.
class TestPluginRegistry : public RuntimeRegistry<TestPluginRegistry, std::string, TestPluginEntry>
{
};

}
