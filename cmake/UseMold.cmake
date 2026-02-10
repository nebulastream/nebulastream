# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Linker
option(NES_USE_MOLD_IF_AVAILABLE "Uses mold for linking if it is available" ON)
find_program(MOLD_EXECUTABLE mold)
if(MOLD_EXECUTABLE AND ${NES_USE_MOLD_IF_AVAILABLE})
    message(STATUS "Using mold linker")
    add_link_options("-fuse-ld=mold")

    # Currently, we expect all symbols to be present at build time. This shifts errors due to missing symbols
    # from crashing the application at runtime to errors at link time.
    add_link_options("-Wl,--no-undefined")
elseif (${NES_USE_MOLD_IF_AVAILABLE})
    message(STATUS "Mold is not available falling back to default linker")
endif ()

