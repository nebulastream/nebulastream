# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-output-formatters
        DefaultValueSerializers.cpp
        ValueSerializerRegistry.cpp
)

add_registry_entry(ValueSerializer DefaultINT8)
add_registry_entry(ValueSerializer DefaultINT16)
add_registry_entry(ValueSerializer DefaultINT32)
add_registry_entry(ValueSerializer DefaultINT64)
add_registry_entry(ValueSerializer DefaultUINT8)
add_registry_entry(ValueSerializer DefaultUINT16)
add_registry_entry(ValueSerializer DefaultUINT32)
add_registry_entry(ValueSerializer DefaultUINT64)
add_registry_entry(ValueSerializer DefaultF32)
add_registry_entry(ValueSerializer DefaultF64)
add_registry_entry(ValueSerializer DefaultBOOL)
add_registry_entry(ValueSerializer DefaultCHAR)
add_registry_entry(ValueSerializer DefaultVARSIZED)
