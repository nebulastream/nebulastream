
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if (NES_ENABLE_THREAD_SANITIZER)
    MESSAGE(STATUS "Enabling Thread Sanitizer")
    add_compile_options(-fsanitize=thread)
    add_link_options(-fsanitize=thread)
elseif (NES_ENABLE_UB_SANITIZER)
    MESSAGE(STATUS "Enabling UB Sanitizer")
    add_compile_options(-fsanitize=undefined)
    add_link_options(-fsanitize=undefined)
elseif (NES_ENABLE_ADDRESS_SANITIZER)
    MESSAGE(STATUS "Enabling Address Sanitizer")
    add_compile_options(-fsanitize=address)
    add_link_options(-fsanitize=address)
else ()
    MESSAGE(STATUS "Enabling No Sanitizer")
endif ()
