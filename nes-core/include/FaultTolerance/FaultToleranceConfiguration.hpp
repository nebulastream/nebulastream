
/*Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

#ifndef NES_FAULTTOLERANCECONFIGURATION_HPP_
#define NES_FAULTTOLERANCECONFIGURATION_HPP_

#include <Common/Location.hpp>
#include <Configurations/BaseConfiguration.hpp>
#include <Configurations/ConfigurationOption.hpp>
#include <Configurations/Worker/LocationFactory.hpp>
#include <Configurations/Worker/PhysicalSourceFactory.hpp>
#include <Configurations/Worker/QueryCompilerConfiguration.hpp>
#include <Runtime/QueryExecutionMode.hpp>
#include <Spatial/LocationProviderCSV.hpp>
#include <Util/Experimental/LocationProviderType.hpp>
#include <map>
#include <string>

namespace NES {


        class FaultToleranceConfiguration;
        using FaultToleranceConfigurationPtr = std::shared_ptr<FaultToleranceConfiguration>;

        /**
* @brief object for storing worker configuration
*/
        class FaultToleranceConfiguration{
          public:

            float getAckInterval() const;
            static std::shared_ptr<FaultToleranceConfiguration> create() { return std::make_shared<FaultToleranceConfiguration>(); }
            FaultToleranceType getProcessingGuarantee() const;
            int getTupleSize() const;
            int getIngestionRate() const;
            int getAckRate() const;
            int getAckSize() const;
            float getCheckpointSize() const;
            float getTotalAckSizePerSecond() const;
            float getOutputQueueSize(float delayToDownstream) const;
            float getTimeBetweenAcks() const;
            QueryId getQueryId() const;
            void setProcessingGuarantee(FaultToleranceType processingGuarantee);
            void setTupleSize(int tupleSize);
            void setIngestionRate(int ingestionRate);
            void setAckRate(int ackRate);
            void setAckSize(int ackSize);
            void setQueryId(QueryId queryId);

          private:
            FaultToleranceType processingGuarantee;

            /**
             * t_size
             * tuple size in bytes
             */
            int tuple_size;

            /**
             * n
             * tuple ingestion rate per second
             */
            int ingestion_rate;

            /**
             * i_t
             * acknowledgement / checkpointing interval in tuples (sent every i_t tuples)
             */
            int ack_rate;

            /**
             * a_size
             * acknowledgement tuple / queue-trimming message size
             */
            int ack_size;

            /**
             * QueryId of the relevant queue
             */
            QueryId queryId;
            };
        };
    // namespace NES

#endif// NES_FaultToleranceConfiguration_HPP_
