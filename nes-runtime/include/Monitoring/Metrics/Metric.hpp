/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_RUNTIME_INCLUDE_MONITORING_METRICS_METRIC_HPP_
#define NES_RUNTIME_INCLUDE_MONITORING_METRICS_METRIC_HPP_

#include <Monitoring/Metrics/MetricType.hpp>
#include <Monitoring/MonitoringForwardRefs.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
#include <nlohmann/json.hpp>

namespace NES::Monitoring {

class Metric;
using MetricPtr = std::shared_ptr<Metric>;

/**
* @brief Class specific serialize methods for basic types. The serialize method to write CpuMetricsWrapper into
* the given Schema and TupleBuffer. The prefix specifies a string
* that should be added before each field description in the Schema.
* @param the metric
* @param the schema
* @param the TupleBuffer
* @param the prefix as std::string
*/
void writeToBuffer(const uint64_t& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);
void writeToBuffer(const std::string& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);
void writeToBuffer(const std::shared_ptr<Metric> metric, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief class specific readFromBuffer()
 * @return the value
 */
void readFromBuffer(uint64_t& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);
void readFromBuffer(std::string& metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);
void readFromBuffer(std::shared_ptr<Metric> metrics, Runtime::TupleBuffer& buf, uint64_t tupleIndex);

/**
 * @brief class specific asJson()
 * @return the value
 */
nlohmann::json asJson(uint64_t intMetric);
nlohmann::json asJson(std::string stringMetric);
nlohmann::json asJson(std::shared_ptr<Metric> ptrMetric);

/**
* @brief The metric class is a conceptual superclass that represents all metrics in NES.
* Currently existing metrics are Counter, GaugeCollectors, Histogram and Meter.
*/
class Metric {
  public:
    /**
     * @brief The ctor of the metric, which takes an arbitrary value
     * @param arbitrary parameter of any type
     * @dev too broad to make non-explicit.
    */
    template<typename T>
    explicit Metric(T x) : self(std::make_unique<Model<T>>(std::move(x))), type(MetricType::UnknownMetric) {}
    template<typename T>
    explicit Metric(T x, MetricType type) : self(std::make_unique<Model<T>>(std::move(x))), type(type) {}
    ~Metric() = default;

    /**
     * @brief copy ctor to properly handle the templated values
     * @param the metric
    */
    Metric(const Metric& x) : self(x.self->copy()){};
    Metric(Metric&&) noexcept = default;

    /**
     * @brief assign operator for metrics to avoid unnecessary copies
    */
    Metric& operator=(const Metric& x) { return *this = Metric(x); }
    Metric& operator=(Metric&& x) noexcept = default;

    /**
     * @brief This method returns the originally stored metric value, e.g. int, string, GaugeCollectors
     * @tparam the type of the value
     * @return the value
    */
    template<typename T>
    [[nodiscard]] T& getValue() const {
        return dynamic_cast<Model<T>*>(self.get())->data;
    }

    /**
     * @brief This method returns the type of the stored metric. Note that the according function needs to be
     * defined, otherwise it will be categorized as UnknownType
     * @param the metric
     * @return the type of the metric
    */
    MetricType getMetricType() const { return type; }

    /**
     * @brief This method returns the value of the metric as a JSON.
     * @param metric
     * @return The metric represented as JSON.
    */
    friend nlohmann::json asJson(const Metric& metric) { return metric.self->toJson(); };

    /**
     * @brief This method returns the type of the stored metric. Note that the according function needs to be
     * defined, otherwise it will be categorized as UnknownType
     * @param the metric
     * @return the type of the metric
    */
    friend void writeToBuffer(const Metric& x, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
        x.self->writeToBufferConcept(buf, tupleIndex);
    }

    /**
     * @brief This method returns the type of the stored metric. Note that the according function needs to be
     * defined, otherwise it will be categorized as UnknownType
     * @param the metric
     * @return the type of the metric
    */
    friend void readFromBuffer(const Metric& x, Runtime::TupleBuffer& buf, uint64_t tupleIndex) {
        return x.self->readFromBufferConcept(buf, tupleIndex);
    }

  private:
    /**
     * @brief Abstract superclass that represents the conceptual features of a metric
    */
    struct ConceptT {
        ConceptT() = default;
        virtual ~ConceptT() = default;
        [[nodiscard]] virtual std::unique_ptr<ConceptT> copy() const = 0;

        /**
         * @brief Returns the values of a metric as JSON.
         */
        [[nodiscard]] virtual nlohmann::json toJson() const = 0;

        /**
         * @brief The serialize concept to enable polymorphism across different metrics to make them serializable.
        */
        virtual void writeToBufferConcept(Runtime::TupleBuffer&, uint64_t tupleIndex) = 0;

        /**
         * @brief The deserialize concept to enable polymorphism across different metrics to make them deserializable.
        */
        virtual void readFromBufferConcept(Runtime::TupleBuffer&, uint64_t tupleIndex) = 0;
    };

    /**
     * @brief Child class of concept that contains the actual metric value.
     * @tparam T
    */
    template<typename T>
    struct Model final : ConceptT {
        explicit Model(T x) : data(std::move(x)){};

        [[nodiscard]] std::unique_ptr<ConceptT> copy() const override { return std::make_unique<Model>(*this); }

        [[nodiscard]] nlohmann::json toJson() const override { return asJson(data); }

        void writeToBufferConcept(Runtime::TupleBuffer& buf, uint64_t tupleIndex) override {
            writeToBuffer(data, buf, tupleIndex);
        }

        void readFromBufferConcept(Runtime::TupleBuffer& buf, uint64_t tupleIndex) override {
            readFromBuffer(data, buf, tupleIndex);
        }

        T data;
    };

    std::unique_ptr<ConceptT> self;
    MetricType type;
};

}// namespace NES::Monitoring

#endif  // NES_RUNTIME_INCLUDE_MONITORING_METRICS_METRIC_HPP_
