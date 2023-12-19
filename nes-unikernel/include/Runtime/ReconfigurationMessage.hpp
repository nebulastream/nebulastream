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

#ifndef NES_RECONFIGURATIONMESSAGE_HPP
#define NES_RECONFIGURATIONMESSAGE_HPP
#include <Identifiers.hpp>
#include <Runtime/ReconfigurationType.hpp>
#include <any>

namespace NES::Runtime {
class ReconfigurationMessage {
  public:
    /**
     * @brief create a reconfiguration task that will be used to kickstart the reconfiguration process
     * @param parentPlanId the owning plan id
     * @param type what kind of reconfiguration we want
     * @param instance the target of the reconfiguration
     * @param userdata extra information to use in this reconfiguration
     */
    explicit ReconfigurationMessage(const QueryId queryId,
                                    const QuerySubPlanId parentPlanId,
                                    ReconfigurationType type,
                                    [[maybe_unused]] void* unused_instance,
                                    std::any&& userdata = nullptr)
        : type(type), queryId(queryId), parentPlanId(parentPlanId), userdata(std::move(userdata)) {
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
    }

    /**
     * @brief create a reconfiguration task that will be passed to every running thread
     * @param other the task we want to issue (created using the other ctor)
     * @param numThreads number of running threads
     * @param instance the target of the reconfiguration
     * @param userdata extra information to use in this reconfiguration
     * @param blocking whether the reconfiguration must block for completion
     */
    explicit ReconfigurationMessage(const QueryId queryId,
                                    const QuerySubPlanId parentPlanId,
                                    ReconfigurationType type,
                                    [[maybe_unused]] void* unused_instance,
                                    [[maybe_unused]] uint64_t numThreads,
                                    std::any&& userdata = nullptr,
                                    [[maybe_unused]] bool blocking = false)
        : type(type), queryId(queryId), parentPlanId(parentPlanId), userdata(std::move(userdata)) {
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
    }

    /**
     * @brief create a reconfiguration task that will be passed to every running thread
     * @param other the task we want to issue (created using the other ctor)
     * @param numThreads number of running threads
     * @param blocking whether the reconfiguration must block for completion
     */
    explicit ReconfigurationMessage(const ReconfigurationMessage& other,
                                    [[maybe_unused]] uint64_t numThreads,
                                    [[maybe_unused]] bool blocking = false)
        : ReconfigurationMessage(other) {
        NES_ASSERT(this->userdata.has_value(), "invalid userdata");
    }

    /**
     * @brief copy constructor
     * @param that
     */
    ReconfigurationMessage(const ReconfigurationMessage& that)
        : type(that.type), queryId(that.queryId), parentPlanId(that.parentPlanId), userdata(that.userdata) {
        // nop
    }

    /**
     * @brief get the reconfiguration type
     * @return the reconfiguration type
     */
    [[nodiscard]] ReconfigurationType getType() const { return type; }

    /**
     * @brief get the target plan id
     * @return the query id
     */
    [[nodiscard]] QueryId getQueryId() const { return queryId; }

    /**
     * @brief get the target plan id
     * @return the plan id
     */
    [[nodiscard]] QuerySubPlanId getParentPlanId() const { return parentPlanId; }

    /**
     * @brief Provides the userdata installed in this reconfiguration descriptor
     * @tparam T the type of the reconfiguration's userdata
     * @return the user data value or error if that is not set
     */
    template<typename T>
    [[nodiscard]] T getUserData() const {
        NES_ASSERT2_FMT(userdata.has_value(), "invalid userdata");
        return std::any_cast<T>(userdata);
    }

  private:
    /// type of the reconfiguration
    ReconfigurationType type;

    /// owning plan id
    const QueryId queryId;

    /// owning plan id
    const QuerySubPlanId parentPlanId;

    /// custom data
    std::any userdata;
};
}// namespace NES::Runtime
#endif//NES_RECONFIGURATIONMESSAGE_HPP
