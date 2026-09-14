# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

include(${PROJECT_SOURCE_DIR}/cmake/RuntimeRegistrationUtil.cmake)
include(${PROJECT_SOURCE_DIR}/cmake/UnreflectionRegistrationUtil.cmake)

find_package(reflectcpp CONFIG REQUIRED)

# Logical function: contributes to the factory and the serialized plan reconstruction registries
# owned by nes-logical-operators.
add_library(ft_preprocess_slot_logical_function_plugin_library STATIC FtPreprocessSlotLogicalFunction.cpp)
target_link_libraries(ft_preprocess_slot_logical_function_plugin_library PRIVATE nes-logical-operators reflectcpp::reflectcpp)
target_include_directories(ft_preprocess_slot_logical_function_plugin_library PUBLIC include PRIVATE .)
link_plugin_library(nes-logical-operators ft_preprocess_slot_logical_function_plugin_library)
# plugin_name matches the class basename (for header derivation); KEY is the actual SQL-visible
# registry lookup name, same split Conditional's own convention documents for Absolute/Abs-style
# mismatches (see add_registry_entry's doc comment in cmake/RuntimeRegistrationUtil.cmake).
add_registry_entry(LogicalFunction FtPreprocessSlot KEY FT_PREPROCESS_SLOT)
add_unreflection_entry(LogicalFunction FtPreprocessSlot KEY FT_PREPROCESS_SLOT)

# Physical function: contributes to the factory registry owned by nes-physical-operators. Vendors
# stb_image.h (public domain, single header) for JPEG decode -- i/cam's frames are baseline JPEG,
# and NES has no image-decode dependency of its own to reuse (see the plan doc in the FischerTechnik
# Dropbox folder for why stb_image over a formal libjpeg-turbo vcpkg dependency). Also does its own
# base64 decode via OpenSSL (same library FromBase64PhysicalFunction uses) after stripping the
# i/cam data-URI's "data:image/jpeg;base64," prefix -- FROM_BASE64 alone corrupts the image bytes
# because that prefix is not valid base64.
find_package(OpenSSL REQUIRED)
add_library(ft_preprocess_slot_physical_function_plugin_library STATIC FtPreprocessSlotPhysicalFunction.cpp)
target_link_libraries(ft_preprocess_slot_physical_function_plugin_library PRIVATE nes-physical-operators OpenSSL::Crypto)
target_include_directories(ft_preprocess_slot_physical_function_plugin_library PUBLIC include PRIVATE .)
# SYSTEM so the vendored header's own warnings (unrelated to our JPEG-only usage) don't trip -Werror.
target_include_directories(ft_preprocess_slot_physical_function_plugin_library SYSTEM PRIVATE thirdparty)
link_plugin_library(nes-physical-operators ft_preprocess_slot_physical_function_plugin_library)
add_registry_entry(PhysicalFunction FtPreprocessSlot KEY FT_PREPROCESS_SLOT)

# Each generated glue translation unit includes the plugin header by basename, and the headers of
# this plugin sit one directory deeper than the plugin root that the glue picks up by itself.
foreach (registry IN ITEMS LogicalFunction LogicalFunctionUnreflection PhysicalFunction)
    get_property(glue_lib GLOBAL PROPERTY "${registry}_RUNTIME_REGISTRY_GLUE_LIB")
    target_include_directories(${glue_lib} PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include/Functions)
endforeach ()
