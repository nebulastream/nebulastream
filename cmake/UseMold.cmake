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
#
# Plugin init hooks live in a linker section and plugin-bearing static archives
# are retained with WHOLE_ARCHIVE. Mold and lld both handle the resulting archive cycles.
option(NES_USE_MOLD_IF_AVAILABLE "Use mold (or lld as fallback) for linking if available" ON)
find_program(MOLD_EXECUTABLE mold)
find_program(LLD_EXECUTABLE NAMES ld.lld lld)
if(NES_USE_MOLD_IF_AVAILABLE AND MOLD_EXECUTABLE)
    message(STATUS "Using mold linker")
    add_link_options("-fuse-ld=mold")
    add_link_options("-Wl,--no-undefined")
elseif(NES_USE_MOLD_IF_AVAILABLE AND LLD_EXECUTABLE)
    message(STATUS "Using lld linker (mold not found)")
    add_link_options("-fuse-ld=lld")
    add_link_options("-Wl,--no-undefined")
elseif(NES_USE_MOLD_IF_AVAILABLE)
    message(STATUS "Neither mold nor lld available; falling back to the default linker. Plugin archive cycles may fail to resolve.")
endif()
