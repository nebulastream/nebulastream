#ifndef NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
#define NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_

namespace NES {

class LogicalOperatorNode;
typedef std::shared_ptr<LogicalOperatorNode> LogicalOperatorNodePtr;

class DataSource;
typedef std::shared_ptr<DataSource> DataSourcePtr;

class ConvertPhysicalToLogicalSource {

  public:
    static LogicalOperatorNodePtr createDataSource(DataSourcePtr dataSource);

  private:
    ConvertPhysicalToLogicalSource() = default;
};

}


#endif //NES_IMPL_NODES_PHASES_CONVERTPHYSICALTOLOGICALSOURCE_HPP_
