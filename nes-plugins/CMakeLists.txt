# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/PluginRegistrationUtil.cmake)

# NOTE: Since these plugins are part of the main NebulaStream repo,
#       we apply the same care and test coverage requirements to these plugins that we apply to the rest of the system.

# activate optional plugins and add the path "THE/PATH" to the build (adding all dependencies of the optional plugin)
option(NES_PLUGIN_TCP_SOURCE "Enable TCP Source plugin" ON)
option(NES_PLUGIN_VOID_SINK "Enable Void Sink plugin" ON)
option(NES_PLUGIN_JSON_INPUT_FORMATTER "Enable JSON Input Formatter plugin" ON)
option(NES_PLUGIN_JSON_OUTPUT_FORMATTER "Enable JSON Output Formatter plugin" ON)

activate_optional_plugin("Sources/TCPSource" ${NES_PLUGIN_TCP_SOURCE})
activate_optional_plugin("Sinks/VoidSink" ${NES_PLUGIN_VOID_SINK})
activate_optional_plugin("InputFormatters/JSONInputFormatter" ${NES_PLUGIN_JSON_INPUT_FORMATTER})
activate_optional_plugin("OutputFormatters/JSONOutputFormatter" ${NES_PLUGIN_JSON_OUTPUT_FORMATTER})

# Maps each optional plugin to the systest group its .test files are tagged with. A plugin-dependent
# systest under nes-systests/plugins/ carries its plugin's group(s); when the plugin is disabled the
# group is written into the generated disable_current.yaml (see nes-systests/systest/CMakeLists.txt) so
# the systest runner excludes those tests. This is the only place that knows the OFF plugins, because a
# disabled plugin's subdirectory is never added. Extend this map when adding a new optional plugin that
# ships systests.
set(NES_PLUGIN_SYSTEST_GROUP_MAP
        NES_PLUGIN_TCP_SOURCE TCP
        NES_PLUGIN_VOID_SINK Void
        NES_PLUGIN_JSON_INPUT_FORMATTER JSON
        NES_PLUGIN_JSON_OUTPUT_FORMATTER JSONOutput
)
set(disabledPluginGroups "")
list(LENGTH NES_PLUGIN_SYSTEST_GROUP_MAP mapLength)
math(EXPR lastPairIndex "${mapLength} - 1")
foreach (optionIndex RANGE 0 ${lastPairIndex} 2)
    math(EXPR groupIndex "${optionIndex} + 1")
    list(GET NES_PLUGIN_SYSTEST_GROUP_MAP ${optionIndex} pluginOption)
    list(GET NES_PLUGIN_SYSTEST_GROUP_MAP ${groupIndex} pluginGroup)
    if (NOT ${pluginOption})
        list(APPEND disabledPluginGroups ${pluginGroup})
    endif ()
endforeach ()
set_property(GLOBAL PROPERTY NES_DISABLED_PLUGIN_GROUPS "${disabledPluginGroups}")
