#ifndef NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
#define NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_

namespace NES {

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

/**
 * @brief This class is responsible for creating the Logical source from physical sources
 */
class ConvertPhysicalToLogicalSource {

  public:
    /**
     * @brief This method takes input as one of the several physical source defined in the system and output the corresponding
     * logical source descriptor
     * @param dataSource the input data source object defining the physical source
     * @return the logical source descriptor
     */
    static SourceDescriptorPtr createSourceDescriptor(DataSourcePtr dataSource);

  private:
    ConvertPhysicalToLogicalSource() = default;
};

}// namespace NES

#endif//NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
