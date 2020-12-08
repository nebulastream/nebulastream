# Copyright (C) 2020 by the NebulaStream project (https://nebula.stream)

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_source_nes(
        NodeEngine.cpp
        NodeStatsProvider.cpp
        QueryManager.cpp
        BufferManager.cpp
        ThreadPool.cpp
        Task.cpp
        TupleBuffer.cpp
        detail/TupleBufferImpl.cpp
        SignalHandling.cpp
        QueryStatistics.cpp
        NesThread.cpp
        WorkerContext.cpp
)
add_subdirectory(MemoryLayout)
add_subdirectory(Execution)