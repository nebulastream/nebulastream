#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_

#include <memory>

namespace NES {

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

class DataSink;
typedef std::shared_ptr<DataSink> DataSinkPtr;

class ConvertPhysicalToLogicalSink {

  public:
    static SinkDescriptorPtr createSinkDescriptor(DataSinkPtr dataSink);

  private:
    ConvertPhysicalToLogicalSink() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_
