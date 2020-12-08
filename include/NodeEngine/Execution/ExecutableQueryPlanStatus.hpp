/*
    Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

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

#ifndef NES_INCLUDE_NODEENGINE_EXECUTION_EXECUTABLEQUERYPLANSTATUS_HPP_
#define NES_INCLUDE_NODEENGINE_EXECUTION_EXECUTABLEQUERYPLANSTATUS_HPP_
namespace NES::NodeEngine::Execution {
enum ExecutableQueryPlanStatus {
    Created,
    Deployed,// Created->Deployed when calling setup()
    Running, // Deployed->Running when calling start()
    Finished,
    Stopped,// Running->Stopped when calling stop() and in Running state
    ErrorState,
    Invalid
};
}

#endif//NES_INCLUDE_NODEENGINE_EXECUTION_EXECUTABLEQUERYPLANSTATUS_HPP_
