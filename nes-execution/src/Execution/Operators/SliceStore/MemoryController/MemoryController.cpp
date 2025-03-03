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

#include <Execution/Operators/SliceStore/MemoryController/MemoryController.hpp>

namespace NES::Runtime::Execution
{

FileWriter MemoryController::getLeftFileWriter(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "left_" << sliceEnd << "_" << threadId << ".dat";
    return FileWriter(ss.str());
}

FileWriter MemoryController::getRightFileWriter(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "right_" << sliceEnd << "_" << threadId << ".dat";
    return FileWriter(ss.str());
}

FileReader MemoryController::getLeftFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "left_" << sliceEnd << "_" << threadId << ".dat";
    return FileReader(ss.str());
}

FileReader MemoryController::getRightFileReader(const SliceEnd sliceEnd, const WorkerThreadId threadId)
{
    std::stringstream ss;
    ss << "right_" << sliceEnd << "_" << threadId << ".dat";
    return FileReader(ss.str());
}

}
