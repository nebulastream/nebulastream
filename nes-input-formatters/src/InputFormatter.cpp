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

#include <Identifiers/Identifiers.hpp>
#include <InputFormatters/InputFormatter.hpp>
#include <Runtime/TupleBuffer.hpp>
#include <Util/Logger/Logger.hpp>


// Nomenclature:
// - rTB: raw tuple buffer
// - fTB: formatted tuple buffer
// - full tuple: entire tuple is in exactly 1 rTB
// - partial tuple: tuple spans across rTBs
// - trTB: tokenized rTB
// - TupDel: tuple delimiter
//
// Main Exploit: We can parse full tuples in parallel across rTBs as long as the resulting fTBs link the tuples correctly to the original TB (sequence numbers)
//
// Cases:
// 1. [x] [..] .. (rTB is first in sequence)
// 2. .. [..] [x] [..] .. (rTB is sandwiched)
// 3. .. [..] [x] (rTB is last in sequence)
//
// Two threads can write to fTB in parallel, if offsets are correct and output schemas
//
// Generalize case 2 to also handle 1/3:
//     1. Parse until first tupleDelimiter
//     2. Check if pre-slot is locked
//     If it is not locked:
//         3.1. If is head, continue
//         3.2 If not, check if slot contains fTB
//         3.2.1 If contains sTB, take, write from offset
//
//         Todo: is offset calculation necessary? Other thread can just write, is sTB even necessary?
//
//         3.2.2 If not, getBuffer -> fTB, create sTB(fTB, offset(tuple size - offset of first TupDel)), start writing from offset
//         4. Continue until last (partial) tuple
//         If it is locked:
//             3. Wait until lock is free, then lock
//             5. take sTB from slot, then free lock
//             6. Start writing first tuple from offset
//             7. Continue until last (partial) tuple
//             -----
//             (Before parsing last (partial) tuple)
//             5./8. Emit current fTB
//             9. Check if post-slot is locked
//             9.1 If not, getBuffer -> fTB, create sTB(fTB, offset(tuple size - size of partial tuple)), start writing (to offset), finish
//             9.2.1 If is locked, wait until lock is free, get lock, get sTB from lock
//             9.2.2 write to sTB, finish

namespace NES::InputFormatters {

// Todo: think about setup/stop
// Todo: implement interface with
// - processBeginningPartialTupleSynchronized()
// - processEndingPartialTupleSynchronized()
// - processMiddleTuples()
// - execute, which calls in order:
//   - processBeginningPartialTupleSynchronized
//      - processes first tuple; synchronizes with prior task if necessary (first byte(s) of buffer is not TupleDelimiter)
//   - processMiddleTuples
//      - asynchronously process all tuples between the first and the last TupleDelimiter
//   - processEndingPartialTupleSynchronized
//      - process last tuple; synchronize with subsequent task (even if last byte is TupleDelimiter,
//      the next task must know, or potentially the next task already wrote an entire tuple that the current task must process)
// The
ExecutionResult InputFormatter::execute(
    Memory::TupleBuffer& inputTupleBuffer,
    Runtime::Execution::PipelineExecutionContext& pipelineExecutionContext,
    Runtime::WorkerContext& workerContext)
{
    parseTupleBufferRaw(inputTupleBuffer, pipelineExecutionContext, workerContext, inputTupleBuffer.getNumberOfTuples());
    return ExecutionResult::Ok;
}
}