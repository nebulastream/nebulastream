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

#include "FaultTolerance/FaultToleranceConfiguration.hpp"
#include "Configurations/ConfigurationOption.hpp"
#include "Util/Logger/Logger.hpp"
#include "Util/UtilityFunctions.hpp"
#include "Util/yaml/Yaml.hpp"
#include <filesystem>
#include <string>
#include <utility>

namespace NES {

FaultToleranceType FaultToleranceConfiguration::getProcessingGuarantee() const { return processingGuarantee; }
/**
 * t_size
 * @return tuple size in bytes
 */
int FaultToleranceConfiguration::getTupleSize() const { return tuple_size; }

/**
 * n
 * @return tuple ingestion rate per seconds
 */
int FaultToleranceConfiguration::getIngestionRate() const { return ingestion_rate; }

/**
 * i_t
 * @return acknowledgement / checkpointing interval in tuples (one tuple sent every <ackRate> input tuples)
 */
int FaultToleranceConfiguration::getAckRate() const { return ack_rate; }

/**
 * a_size
 * @return acknowledgement tuple size in bytes
 */
int FaultToleranceConfiguration::getAckSize() const { return ack_size; }

/*
 * ================= DERIVATIVE VALUES ==============================
 */

/**
 * i_s
 * @return acknowledgement tuple interval in seconds (one tuple sent every <ackInterval> seconds)
 */
float FaultToleranceConfiguration::getAckInterval() const { return ( static_cast<float>(this->ingestion_rate) / static_cast<float>(this->getAckRate())); }

/**
 * c
 * @return checkpoint size in byte
 */
float FaultToleranceConfiguration::getCheckpointSize() const { return (this->ack_rate * this->tuple_size); }

/**
 *
 * @return
 */
float FaultToleranceConfiguration::getTotalAckSizePerSecond() const { return (this->ack_rate * this->ack_size); }

/**
 * t_i
 * @return time between acknowledgement tuples in seconds
 */
float FaultToleranceConfiguration::getTimeBetweenAcks() const { return (static_cast<float>(1) / static_cast<float>(this->ingestion_rate)); }

/**
 * o
 * @param delayToDownstream - network delay to the downstream node in seconds
 * @return output queue size in byte
 */
float FaultToleranceConfiguration::getOutputQueueSize(float delayToDownstream) const {
    return ((this->getTimeBetweenAcks() * this->tuple_size * this->tuple_size) + (delayToDownstream * this->ingestion_rate * this->tuple_size));
}

void FaultToleranceConfiguration::setQueryId(QueryId queryId){ this->queryId = queryId; };

QueryId FaultToleranceConfiguration::getQueryId() const { return this->queryId; };

void FaultToleranceConfiguration::setProcessingGuarantee(FaultToleranceType processingGuarantee) {
    FaultToleranceConfiguration::processingGuarantee = processingGuarantee;
}
void FaultToleranceConfiguration::setTupleSize(int tupleSize) { tuple_size = tupleSize; }
void FaultToleranceConfiguration::setIngestionRate(int ingestionRate) { ingestion_rate = ingestionRate; }
void FaultToleranceConfiguration::setAckRate(int ackRate) { ack_rate = ackRate; }
void FaultToleranceConfiguration::setAckSize(int ackSize) { ack_size = ackSize; }
}// namespace NES
