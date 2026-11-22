# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_files(nes-query-optimizer
        Trait.cpp
        TraitSet.cpp
        JoinImplementationTypeTrait.cpp
        MemoryLayoutTypeTrait.cpp
        OutputOriginIdsTrait.cpp
        FieldMappingTrait.cpp
        FieldOrderingTrait.cpp
        PlacementTrait.cpp)

add_unreflection_entry(Trait JoinImplementationType)
add_unreflection_entry(Trait MemoryLayoutType)
add_unreflection_entry(Trait OutputOriginIds)
add_unreflection_entry(Trait Placement)
add_unreflection_entry(Trait FieldMapping)
add_unreflection_entry(Trait FieldOrdering)
