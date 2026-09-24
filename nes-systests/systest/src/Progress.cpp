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

#include <Progress.hpp>

namespace NES
{

ProgressTracker::ProgressTracker() = default;

ProgressTracker::ProgressTracker(size_t totalQueries) : totalQueries(totalQueries)
{
}

void ProgressTracker::incrementQueryCounter()
{
    ++queryCounter;
}

size_t ProgressTracker::getQueryCounter() const
{
    return queryCounter.load();
}

void ProgressTracker::setTotalQueries(size_t total)
{
    totalQueries = total;
}

size_t ProgressTracker::getTotalQueries() const
{
    return totalQueries;
}

double ProgressTracker::getProgressInPercent() const
{
    return totalQueries > 0 ? (static_cast<double>(queryCounter.load()) * 100.0) / static_cast<double>(totalQueries) : 0.0;
}

void ProgressTracker::reset()
{
    queryCounter.store(0);
}

}
