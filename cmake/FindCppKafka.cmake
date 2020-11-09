# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(NOT CppKafka_FOUND)
  find_library(CppKafka_LIBRARIES
    NAMES libcppkafka.so libcppkafka.dylib
    HINTS /usr/local/lib)

  find_path(CppKafka_INCLUDE_DIRS cppkafka/cppkafka.h /usr/local/include)
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(CppKafka
    DEFAULT_MSG
    CppKafka_LIBRARIES
    CppKafka_INCLUDE_DIRS)
  message("Found cppkafka include dirs: ${CppKafka_INCLUDE_DIRS}")
  message("Found cppkafka library: ${CppKafka_LIBRARIES}")
  message("CppKafka_FOUND: ${CppKafka_FOUND}")
  mark_as_advanced(
    CppKafka_LIBRARIES
    CppKafka_INCLUDE_DIRS
    )

endif()
