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

#ifndef NES_REQUESTTYPE_HPP
#define NES_REQUESTTYPE_HPP

#include <string>

namespace NES{

class RequestType {
  public:
    /**
     * @brief Represents various request types.
     *
     * Registered: Query is registered to be scheduled to the worker nodes (added to the queue).
     * Optimizing: Coordinator is optimizing the query.
     * Running: Query is now running successfully.
     * MarkedForHardStop: A request arrived into the system for stopping a query and system marks the query for stopping (added to the queue).
     * Stopped: Query was explicitly stopped either by system or by user.
     * Failed: Query failed because of some reason.
     */
    enum Value : uint8_t {
        Add = 0,
        Stop,
        Restart,
        Fail,
        Migrate,
        Update
    };

    /**
     * @brief Get query status from string
     * @param queryStatus : string representation of query status
     * @return enum representing query status
     */
    static Value getFromString(const std::string queryStatus);

    /**
     * @brief Get query status in string representation
     * @param queryStatus : enum value of the query status
     * @return string representation of query status
     */
    static std::string toString(const Value queryStatus);

};

}

#endif//NES_REQUESTTYPE_HPP
