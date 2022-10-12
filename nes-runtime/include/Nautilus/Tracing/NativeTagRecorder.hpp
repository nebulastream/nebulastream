#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_NATIVETRACETAGRECORDER_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_NATIVETRACETAGRECORDER_HPP_
#include <Nautilus/Tracing/TagRecorder.hpp>
namespace NES::Nautilus::Tracing {
class NativeTagRecorder : public TagRecorder{
  public:
    TagAddress createStartAddress() override;
    Tag createTag(TagAddress startAddress) override;
};
}
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_NATIVETRACETAGRECORDER_HPP_
