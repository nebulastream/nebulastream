#ifndef NES_INCLUDE_MONITORING_SAMPLINGPROTOCOL_HPP_
#define NES_INCLUDE_MONITORING_SAMPLINGPROTOCOL_HPP_

#include <Monitoring/Metrics/MetricGroup.hpp>
#include <functional>
#include <memory>

namespace NES {

/**
 * @brief
 * @tparam T
 */
class SamplingProtocol {
  public:
    explicit SamplingProtocol(std::function<MetricGroup()>&& samplingFunc) : samplingFunc(samplingFunc) {
    };

    MetricGroup getSample() {
        return samplingFunc();
    };

  private:
    std::function<MetricGroup()> samplingFunc;

};

typedef std::shared_ptr<SamplingProtocol> SamplingProtocolPtr;

}

#endif //NES_INCLUDE_MONITORING_SAMPLINGPROTOCOL_HPP_
