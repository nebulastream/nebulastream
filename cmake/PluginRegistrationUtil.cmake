# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

function(retain_plugin_hooks target)
    set_property(GLOBAL APPEND PROPERTY plugin_startup_targets ${target})
endfunction()

function(finalize_plugin_hooks)
    get_property(components GLOBAL PROPERTY plugin_components)
    get_property(targets GLOBAL PROPERTY plugin_startup_targets)
    foreach (target ${targets})
        foreach (component ${components})
            set_property(TARGET ${target} PROPERTY "LINK_LIBRARY_OVERRIDE_${component}" WHOLE_ARCHIVE)
        endforeach ()
    endforeach ()
endfunction()

function(enable_plugin_component)
    get_filename_component(component "${CMAKE_CURRENT_LIST_DIR}" NAME)
    set_property(GLOBAL APPEND PROPERTY plugin_components ${component})
    target_include_directories(${component} PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/registry/include)
endfunction()
