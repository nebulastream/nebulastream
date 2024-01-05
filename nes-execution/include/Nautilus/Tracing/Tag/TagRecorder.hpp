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

#ifndef NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TAG_TAGRECORDER_HPP_
#define NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TAG_TAGRECORDER_HPP_
#include <Nautilus/Tracing/Tag/Tag.hpp>

namespace NES::Nautilus::Tracing {

/**
 * @brief The tag recorder derives the tag from at a specific execution position.
 * To this end, it derives the backtrace, which is a unique identifier of an instruction.
 */
class TagRecorder final {
  public:
    static constexpr size_t MAX_TAG_SIZE = 60;
    /**
     * @brief Factory to create a new tag recorder.
     * @return TagRecorder
     */
    static inline __attribute__((always_inline)) TagRecorder createTagRecorder() {
        // First we derive the base address, which is the first common address between two bracktraces.
        auto referenceTag1 = createBaseTag();
        auto referenceTag2 = createBaseTag();
        auto baseAddress = getBaseAddress(referenceTag1, referenceTag2);
        return TagRecorder(baseAddress);
    }
    /**
     * @brief Derive the tag of a specific instruction and returns the tag pointer, which uniquely identifies this instruction.
     * @return Tag*
     */
    [[nodiscard]] inline __attribute__((always_inline)) Tag* createTag() { return this->createReferenceTag(startAddress); }

  private:
    /**
     * @brief Create a new tag recorder with a fixed start address.
     * @param startAddress
     */
    explicit TagRecorder(TagAddress startAddress);
    static TagAddress getBaseAddress(TagVector& tag1, TagVector& tag2);
    static TagVector createBaseTag();
    Tag* createReferenceTag(TagAddress startAddress);
    // The start address, which is used as the start to calculate tags for sub instructions.
    const TagAddress startAddress;
    // The tag recorder stores the individual tags in a trie of addresses, to minimize space consumption.
    Tag rootTagThreeNode = Tag();
};
}// namespace NES::Nautilus::Tracing

#endif// NES_RUNTIME_INCLUDE_NAUTILUS_TRACING_TAG_TAGRECORDER_HPP_
