# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_plugin(DefaultINT8 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultINT16 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultINT32 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultINT64 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultUINT8 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultUINT16 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultUINT32 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultUINT64 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultF32 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultF64 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultBOOL ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultCHAR ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(DefaultVARSIZED ValueDeserializer DefaultValueDeserializers.cpp)

add_plugin(NullableDefaultINT8 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultINT16 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultINT32 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultINT64 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultUINT8 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultUINT16 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultUINT32 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultUINT64 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultF32 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultF64 ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultBOOL ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultCHAR ValueDeserializer DefaultValueDeserializers.cpp)
add_plugin(NullableDefaultVARSIZED ValueDeserializer DefaultValueDeserializers.cpp)
