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

#ifndef NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_ZMQSINK_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_ZMQSINK_HPP_

#include <cstdint>
#include <memory>
#include <string>
#include <zmq.hpp>

#include <Sinks/Mediums/SinkMedium.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

class ZmqSink : public SinkMedium {

  public:
    //TODO: remove internal flag once the new network stack is in place
    ZmqSink(SinkFormatPtr format,
            Runtime::NodeEnginePtr nodeEngine,
            uint32_t numOfProducers,
            const std::string& host,
            uint16_t port,
            bool internal,
            SharedQueryId sharedQueryId,
            DecomposedQueryId decomposedQueryId,
            DecomposedQueryPlanVersion decomposedQueryVersion,
            FaultToleranceType faultToleranceType = FaultToleranceType::NONE,
            uint64_t numberOfOrigins = 1);
    ~ZmqSink() override;

    bool writeData(Runtime::TupleBuffer& inputBuffer, Runtime::WorkerContextRef) override;
    void setup() override;
    void shutdown() override;
    std::string toString() const override;

    /**
     * @brief Get zmq sink port
     */
    int getPort() const;

    /**
     * @brief Get Zmq address name
     */
    std::string getHost() const;

    /**
    * @brief method to return the type of medium
    * @return type of medium
    */
    SinkMediumTypes getSinkMediumType() override;

  private:
    std::string host;
    uint16_t port;

    bool connected{false};
    bool internal;
    zmq::context_t context{zmq::context_t(1)};
    zmq::socket_t socket{zmq::socket_t(context, ZMQ_PUSH)};

    bool connect();
    bool disconnect();
};
using ZmqSinkPtr = std::shared_ptr<ZmqSink>;
}// namespace NES

namespace fmt {
template<>
struct formatter<NES::ZmqSink> : formatter<std::string> {
    auto format(const NES::ZmqSink& zmq_sink, format_context& ctx) -> decltype(ctx.out()) {
        return fmt::format_to(ctx.out(), "{} {}", zmq_sink.getHost(), zmq_sink.getPort());
    }
};
}//namespace fmt

#endif// NES_RUNTIME_INCLUDE_SINKS_MEDIUMS_ZMQSINK_HPP_
