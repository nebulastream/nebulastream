#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
#include <Nautilus/Tracing/Tag.hpp>
namespace NES::Nautilus::Tracing {
class TagRecorder {
  public:
    TagRecorder();
    virtual TagAddress createStartAddress() = 0;
    virtual Tag createTag(TagAddress startAddress) = 0;
    virtual ~TagRecorder() = default;
};
}// namespace NES::Nautilus::Tracing

#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_TAGCREATION_HPP_
