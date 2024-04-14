
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-nautilus
        Any.cpp
        Value.cpp
        Boolean.cpp
        InvocationPlugin.cpp
        BooleanInvocationPlugin.cpp
        MemRefInvocationPlugin.cpp
        MemRefUtils.cpp
        Identifier.cpp
        IdentifierInvocationPlugin.cpp
        )

add_subdirectory(Text)
add_subdirectory(List)
add_subdirectory(Integer)
add_subdirectory(Float)
add_subdirectory(TimeStamp)