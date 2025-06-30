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

#pragma once
#include <memory>
#include <Configurations/Worker/SliceCacheConfiguration.hpp>
#include <SliceCache/SliceCache.hpp>
#include <val.hpp>

namespace NES::Util
{

std::unique_ptr<SliceCache> createSliceCache(
    const Configurations::SliceCacheOptions& sliceCacheOptions,
    nautilus::val<OperatorHandler*> globalOperatorHandler,
    const nautilus::val<int8_t*>& startOfSliceEntries);

}
