#ifndef NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
#define NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_

namespace NES {

class SourceDescriptor;
typedef std::shared_ptr<SourceDescriptor> SourceDescriptorPtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class ConvertPhysicalToLogicalSource {

  public:
    static SourceDescriptorPtr createSourceDescriptor(DataSourcePtr dataSource);

  private:
    ConvertPhysicalToLogicalSource() = default;
};

}


#endif //NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
