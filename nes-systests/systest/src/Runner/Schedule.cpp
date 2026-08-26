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

#include <Runner/Schedule.hpp>

#include <algorithm>
#include <cstddef>
#include <functional>
#include <iterator>
#include <vector>

#include <Model/RewrittenTest.hpp>

namespace NES
{

Schedule::File::File(const size_t runnable, const size_t offset, const RewrittenTest& test)
    : test(test), runnable(runnable), offset(offset), done(test.cases.size(), false)
{
}

std::vector<Job> Schedule::File::release()
{
    std::vector<Job> jobs;
    while (cursor < caseCount() and not blocked(cursor))
    {
        jobs.push_back(Job{.runnable = runnable, .index = cursor, .position = offset + cursor});
        ++cursor;
    }
    return jobs;
}

bool Schedule::File::blocked(const size_t index) const
{
    return index > 0 and test.get().cases.at(index).runsAfterPrevious and not done.at(index - 1);
}

Schedule::Schedule(const std::vector<std::reference_wrapper<const RewrittenTest>>& tests)
{
    files.reserve(tests.size());
    for (const RewrittenTest& test : tests)
    {
        files.emplace_back(files.size(), total, test);
        total += test.cases.size();
    }
}

std::vector<Job> Schedule::ready()
{
    std::vector<Job> jobs;
    for (auto& file : files)
    {
        std::ranges::move(file.release(), std::back_inserter(jobs));
    }
    return jobs;
}

std::vector<Job> Schedule::completed(const Job& job)
{
    auto& file = files.at(job.runnable);
    file.markDone(job.index);
    return file.release();
}

}
