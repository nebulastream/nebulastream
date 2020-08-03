#ifndef NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_

#include <memory>
#include <Util/Logger.hpp>

namespace NES {
class MetricValue;

/**
 * @brief The metric types of NES
 * Counter for incrementing and decrementing values
 * Gauge for reading and returning a specific value
 * Histogram that creates a histogram over time
 * Meter that measures an interval between two points in time
 */
enum MetricType {
    CounterType,
    GaugeType,
    HistogramType,
    MeterType,
    UnknownType
};

/**
 * @brief The metric class is a conceptual superclass that represents all metrics in NES.
 * Currently existing metrics are Counter, Gauge, Histogram and Meter.
 */
class Metric {
  public:
    template<typename T>
    Metric(T x, MetricType metricType=UnknownType): self_(std::make_unique<model<T>>(std::move(x))), metricType(metricType){
    }

    Metric(const Metric& x): self_(x.self_->copy_()){
        NES_DEBUG("Metric: Calling copy ctor");
    };

    Metric(Metric&&) noexcept = default;

    Metric& operator=(const Metric& x) {
        return *this=Metric(x);
    }
    Metric& operator=(Metric&& x) noexcept = default;

    template<typename T>
    T& getValue() const{
        return dynamic_cast<model<T>*>(self_.get())->data_;
    }

    MetricType getType() const {
        return this->metricType;
    }

  private:
    struct concept_t {
        virtual ~concept_t() = default;
        virtual std::unique_ptr<concept_t> copy_() const = 0;
    };

    template<typename T>
    struct model final : concept_t {
        explicit model(T x): data_(std::move(x)) { };

        std::unique_ptr<concept_t> copy_() const override {
            return std::make_unique<model>(*this);
        }

        T data_;
    };

    std::unique_ptr<concept_t> self_;
    MetricType metricType;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
