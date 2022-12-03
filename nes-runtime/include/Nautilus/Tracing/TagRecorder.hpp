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
