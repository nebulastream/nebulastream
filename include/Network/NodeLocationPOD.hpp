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
//
// Created by balint on 03.05.21.
//

#ifndef NES_NODELOCATIONPOD_HPP
#define NES_NODELOCATIONPOD_HPP

struct NodeLocationPOD{
    uint64_t nodeId;
    std::string hostname;
    uint32_t port;

};

#endif//NES_NODELOCATIONPOD_HPP
