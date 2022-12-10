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
#include <Nautilus/Tracing/Tag/Tag.hpp>

namespace NES::Nautilus::Tracing {

class TagRecorder final {
  public:
    TagRecorder(TagAddress startAddress);
    static constexpr size_t MAX_TAG_SIZE = 30;
    static inline __attribute__((always_inline)) TagRecorder createTagRecorder() {
        auto referenceTag1 = createBaseTag();
        auto referenceTag2 = createBaseTag();
        auto baseAddress = getBaseAddress(referenceTag1, referenceTag2);
        return {baseAddress};
    }
    [[nodiscard]] inline __attribute__((always_inline)) Tag* createTag() { return this->createReferenceTag(startAddress); }
    ~TagRecorder() = default;

  private:
    static TagAddress getBaseAddress(TagVector& tag1, TagVector& tag2);
    static TagVector createBaseTag();
    Tag* createReferenceTag(TagAddress startAddress);
    const TagAddress startAddress;
    Tag rootTagThreeNode;
};
}// namespace NES::Nautilus::Tracing

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
