# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#    https://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

add_subdirectory(NonKeyedTimeWindow)
add_subdirectory(KeyedTimeWindow)
add_subdirectory(Buckets)

add_source_files(unikernel-export
        WindowProcessingException.cpp
        AppendToSliceStoreAction.cpp
)
add_source_files(nes-runtime
        WindowProcessingException.cpp
        SlidingWindowSliceStore.cpp
        AppendToSliceStoreAction.cpp
        AppendToSliceStoreHandler.cpp
        AbstractSlicePreAggregationHandler.cpp
        ThreadLocalSliceStore.cpp
)
add_source_files(unikernel-lib
        AbstractSlicePreAggregationHandler.cpp
        ThreadLocalSliceStore.cpp
        WindowProcessingException.cpp
)

# KeyedWindows
add_source_files(unikernel-lib
        AppendToSliceStoreHandler.cpp
)

# SlidingWindows
add_source_files(unikernel-lib
        SlidingWindowSliceStore.cpp
)