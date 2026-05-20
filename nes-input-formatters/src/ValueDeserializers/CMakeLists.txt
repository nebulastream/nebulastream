# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-input-formatters ValueDeserializerRegistry.cpp DefaultValueDeserializers.cpp)

add_registry_entry(ValueDeserializer DefaultINT8)
add_registry_entry(ValueDeserializer DefaultINT16)
add_registry_entry(ValueDeserializer DefaultINT32)
add_registry_entry(ValueDeserializer DefaultINT64)
add_registry_entry(ValueDeserializer DefaultUINT8)
add_registry_entry(ValueDeserializer DefaultUINT16)
add_registry_entry(ValueDeserializer DefaultUINT32)
add_registry_entry(ValueDeserializer DefaultUINT64)
add_registry_entry(ValueDeserializer DefaultF32)
add_registry_entry(ValueDeserializer DefaultF64)
add_registry_entry(ValueDeserializer DefaultBOOL)
add_registry_entry(ValueDeserializer DefaultCHAR)
add_registry_entry(ValueDeserializer DefaultVARSIZED)

add_registry_entry(ValueDeserializer NullableDefaultINT8)
add_registry_entry(ValueDeserializer NullableDefaultINT16)
add_registry_entry(ValueDeserializer NullableDefaultINT32)
add_registry_entry(ValueDeserializer NullableDefaultINT64)
add_registry_entry(ValueDeserializer NullableDefaultUINT8)
add_registry_entry(ValueDeserializer NullableDefaultUINT16)
add_registry_entry(ValueDeserializer NullableDefaultUINT32)
add_registry_entry(ValueDeserializer NullableDefaultUINT64)
add_registry_entry(ValueDeserializer NullableDefaultF32)
add_registry_entry(ValueDeserializer NullableDefaultF64)
add_registry_entry(ValueDeserializer NullableDefaultBOOL)
add_registry_entry(ValueDeserializer NullableDefaultCHAR)
add_registry_entry(ValueDeserializer NullableDefaultVARSIZED)
