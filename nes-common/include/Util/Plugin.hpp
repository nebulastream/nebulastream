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

namespace NES
{

struct PluginInit
{
    const char* name;
    void (*init)();
};

extern "C" {
extern const PluginInit __start_nes_plugin_init[] __attribute__((weak));
extern const PluginInit __stop_nes_plugin_init[] __attribute__((weak));
}

void initializePlugins();

}

#define NES_DETAIL_CONCAT_IMPL(left, right) left##right
#define NES_DETAIL_CONCAT(left, right) NES_DETAIL_CONCAT_IMPL(left, right)
#define NES_DETAIL_ADD_PLUGIN(NAME, ID) \
    static void NES_DETAIL_CONCAT(nesPluginInit_, ID)(); \
    [[gnu::used, gnu::section("nes_plugin_init")]] constexpr ::NES::PluginInit NES_DETAIL_CONCAT(nesPlugin_, ID){ \
        NAME, NES_DETAIL_CONCAT(nesPluginInit_, ID)}; \
    static void NES_DETAIL_CONCAT(nesPluginInit_, ID)()

#define ADD_PLUGIN(NAME) NES_DETAIL_ADD_PLUGIN(NAME, __COUNTER__)
