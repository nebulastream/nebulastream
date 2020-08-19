#ifndef NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
#define NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_

#include <Monitoring/Metrics/MetricType.hpp>
#include <Util/Logger.hpp>
#include <memory>

namespace NES {
class TupleBuffer;
class Schema;

template<typename T>
MetricType getMetricType(const T&) {
    return UnknownType;
}

/**
 * @brief Class specific serialize methods for basic types. The serialize method to write CpuMetrics into
 * the given Schema and TupleBuffer. The prefix specifies a string
 * that should be added before each field description in the Schema.
 * @param the metric
 * @param the schema
 * @param the TupleBuffer
 * @param the prefix as std::string
 */
void serialize(uint64_t metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix);
void serialize(std::string metric, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix);

/**
 * @brief The metric class is a conceptual superclass that represents all metrics in NES.
 * Currently existing metrics are Counter, Gauge, Histogram and Meter.
 */
class Metric {
  public:
    /**
     * @brief The ctor of the metric, which takes an arbitrary value
     * @param arbitrary parameter of any type
     */
    template<typename T>
    Metric(T x) : self(std::make_unique<model<T>>(std::move(x))) {
    }

    /**
     * @brief copy ctor to properly handle the templated values
     * @param the metric
     */
    Metric(const Metric& x) : self(x.self->copy()){};
    Metric(Metric&&) noexcept = default;

    /**
     * @brief assign operator for metrics to avoid unnecessary copies
     */
    Metric& operator=(const Metric& x) {
        return *this = Metric(x);
    }
    Metric& operator=(Metric&& x) noexcept = default;

    /**
     * @brief This method returns the originally stored metric value, e.g. int, string, Gauge
     * @tparam the type of the value
     * @return the value
     */
    template<typename T>
    T& getValue() const {
        return dynamic_cast<model<T>*>(self.get())->data;
    }

    /**
     * @brief This method returns the type of the stored metric. Note that the according function needs to be
     * defined, otherwise it will be categorized as UnknownType
     * @param the metric
     * @return the type of the metric
     */
    friend MetricType getMetricType(const Metric& metric) {
        return metric.self->getType();
    }

    /**
     * @brief This method returns the type of the stored metric. Note that the according function needs to be
     * defined, otherwise it will be categorized as UnknownType
     * @param the metric
     * @return the type of the metric
     */
    friend void serialize(const Metric& x, std::shared_ptr<Schema> schema, TupleBuffer& buf, const std::string& prefix) {
        x.self->serializeC(schema, buf, prefix);
    }

  private:
    /**
     * @brief Abstract superclass that represents the conceptual features of a metric
     */
    struct concept_t {
        virtual ~concept_t() = default;
        virtual std::unique_ptr<concept_t> copy() const = 0;
        virtual MetricType getType() const = 0;

        /**
         * @brief The serialize concept to enable polymorphism across different metrics to make them serializable.
         */
        virtual void serializeC(std::shared_ptr<Schema>, TupleBuffer&, std::string) = 0;
    };

    /**
     * @brief Child class of concept that contains the actual metric value.
     * @tparam T
     */
    template<typename T>
    struct model final : concept_t {
        explicit model(T x) : data(std::move(x)){};

        std::unique_ptr<concept_t> copy() const override {
            return std::make_unique<model>(*this);
        }

        MetricType getType() const override {
            return getMetricType(data);
        }

        void serializeC(std::shared_ptr<Schema> schema, TupleBuffer& buf, std::string prefix) override {
            serialize(data, schema, buf, prefix);
        }

        T data;
        MetricType type;
    };

    std::unique_ptr<concept_t> self;
};

}// namespace NES

#endif//NES_INCLUDE_MONITORING_METRICS_METRIC_HPP_
