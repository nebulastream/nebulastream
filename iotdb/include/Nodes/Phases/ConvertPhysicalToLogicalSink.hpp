#ifndef NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_
#define NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_

#include <memory>

namespace NES {

class SinkDescriptor;
typedef std::shared_ptr<SinkDescriptor> SinkDescriptorPtr;

class DataSink;
typedef std::shared_ptr<DataSink> DataSinkPtr;

/**
 * @brief This class is responsible for creating the Logical sink from physical sink
 */
class ConvertPhysicalToLogicalSink {

  public:
    /**
     * @brief This method takes input as one of the several physical sink defined in the system and output the corresponding
     * logical sink descriptor
     * @param dataSink the input data sink object defining the physical sink
     * @return the logical sink descriptor
     */
    static SinkDescriptorPtr createSinkDescriptor(DataSinkPtr dataSink);

  private:
    ConvertPhysicalToLogicalSink() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_OPERATORS_PHYSICALOPERATORS_SOURCES_CONVERTPHYSICALTOLOGICALSINK_HPP_
