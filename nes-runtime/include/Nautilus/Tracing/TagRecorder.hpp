/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
#include <Nautilus/Tracing/Tag.hpp>
namespace NES::Nautilus::Tracing {
class TagRecorder final {
  public:
    static constexpr size_t MAX_TAG_SIZE = 30;
    static inline __attribute__((always_inline)) TagRecorder createTagRecorder() {
        auto referenceTag1 = createBaseTag();
        auto referenceTag2 = createBaseTag();
        auto baseAddress = getBaseAddress(referenceTag1, referenceTag2);
        return TagRecorder(baseAddress);
    }
    inline __attribute__((always_inline)) Tag createTag() const { return createReferenceTag(startAddress); }
    ~TagRecorder() = default;

  private:
    TagRecorder(TagAddress startAddress);
    static TagAddress getBaseAddress(Tag& tag1, Tag& tag2);
    static Tag createBaseTag();
    static Tag createReferenceTag(TagAddress startAddress);
    const TagAddress startAddress;
};
}// namespace NES::Nautilus::Tracing

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
