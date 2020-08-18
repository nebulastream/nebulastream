#ifndef NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
#define NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_

#include <Monitoring/MetricValues/NetworkValues.hpp>
#include <cstdint>
#include <memory>
#include <unordered_map>
#include <vector>

namespace NES {
class Schema;
class TupleBuffer;

class NetworkMetrics {

  public:
    NetworkMetrics() = default;

    // Overloading [] operator to access elements in array style
    NetworkValues& operator[](std::basic_string<char> interfaceName);

    std::vector<std::string> getInterfaceNames();

    unsigned int getInterfaceNum() const;

  private:
    std::unordered_map<std::string, NetworkValues> interfaceMap;
};

/**
 * @brief The serialize method to write NetworkMetrics into the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the NetworkMetrics
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(NetworkMetrics metrics, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix);


}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICVALUES_NETWORKMETRICS_HPP_
