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
    explicit SamplingProtocol(std::function<MetricGroupPtr()>&& samplingFunc) : samplingFunc(samplingFunc){};

    MetricGroupPtr getSample() {
        return samplingFunc();
    };

  private:
    std::function<MetricGroupPtr()> samplingFunc;
};

typedef std::shared_ptr<SamplingProtocol> SamplingProtocolPtr;

}// namespace NES

#endif//NES_INCLUDE_MONITORING_SAMPLINGPROTOCOL_HPP_
