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
# We need a linker that resolves static archives in --start-group / RESCAN style by
# default. Our unreflection registry design (see cmake/UnreflectionRegistrationUtil.cmake)
# puts WHOLE_ARCHIVE'd glue archives in a target's INTERFACE link line, and those glue TUs
# reference back into the parent archive. GNU ld scans archives once left-to-right and
# fails to resolve these back-references; mold and lld both rescan, so either works.
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
    message(STATUS "Neither mold nor lld available; falling back to default linker (likely GNU ld). Link may fail on unreflection registries.")
endif()

