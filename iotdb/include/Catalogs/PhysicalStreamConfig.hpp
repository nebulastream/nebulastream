#ifndef INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_
#define INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_

#include <string>
//#include <caf/all.hpp>

namespace iotdb {

/**
 * @brief this struct covers the information about the attached sensor
 * @param sourceType: string of data source, e.g., OneGeneratorSource or CSVSource
 * @param sourceConf: parameter for the data source, e.g., numberOfProducedBuffer or file path
 * @param physicalStreamName: name of the stream created by this source
 * @param logicalStreamName: name of the logical steam where this physical stream contributes to
 */
struct PhysicalStreamConfig {

  PhysicalStreamConfig() {
    sourceType = "OneGeneratorSource";
    sourceConfig = "1";
    physicalStreamName = "default_physical";
    logicalStreamName = "default_logical";
  }
  ;

  PhysicalStreamConfig(std::string sourceType, std::string sourceConf,
                       std::string physicalStreamName,
                       std::string logicalStreamName)
      : sourceType(sourceType),
        sourceConfig(sourceConf),
        physicalStreamName(physicalStreamName),
        logicalStreamName(logicalStreamName) {
  }
  ;
  std::string sourceType;
  std::string sourceConfig;
  std::string physicalStreamName;
  std::string logicalStreamName;
};

//// PhysicalStreamConfig needs to be serializable
//template <class Inspector>
//typename Inspector::result_type inspect(Inspector& f, PhysicalStreamConfig& x) {
//  return f(meta::type_name("PhysicalStreamConfig"), x.filePath, x.physicalStreamName, x.logicalStreamName);

}
#endif /* INCLUDE_CATALOGS_PHYSICALSTREAMCONFIG_HPP_ */
