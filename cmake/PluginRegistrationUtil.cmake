# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Plugin build utilities. Registration itself lives in cmake/RuntimeRegistrationUtil.cmake;
# this file only wires plugin directories into the build.

# add_plugin(<plugin_path>)
# Adds a plugin directory to the build. Currently a thin wrapper around add_subdirectory; it is
# the per-plugin hook where future cross-cutting plugin logic belongs (e.g. per-plugin build
# options, dynamic-vs-static plugin switches, or verification that all of a plugin's registry
# entries were registered consistently).
function(add_plugin plugin_path)
    message(STATUS "Adding plugin: ${plugin_path} (and all of its dependencies).")
    add_subdirectory(${plugin_path})
endfunction()
