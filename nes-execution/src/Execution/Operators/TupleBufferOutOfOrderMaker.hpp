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


namespace NES::Runtime::Execution::Operators {

class TupleBufferOutOfOrderMaker {
public:

    void open(ExecutionContext&, RecordBuffer& recordBuffer) {
        nautilus::invoke(+[](const uint64_t randomSeconds) {
            delay = random.randint(self.min_delay, self.max_delay);
            sleep(delay);
        }, randomSeconds);
    }


    private:
        uint64_t minDelay, maxDelay;
};
}
