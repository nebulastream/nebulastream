#ifndef NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_BACKTRACETAGRECORDER_HPP_
#define NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_BACKTRACETAGRECORDER_HPP_
#include <Nautilus/Tracing/TagRecorder.hpp>
namespace NES::Nautilus::Tracing {
class BacktraceTagRecorder : public TagRecorder{
  public:
    TagAddress createStartAddress() override;
    Tag createTag(TagAddress startAddress) override;
};
}
#endif//NES_NES_EXECUTION_ENGINE_INCLUDE_NAUTILUS_TRACING_BACKTRACETAGRECORDER_HPP_
