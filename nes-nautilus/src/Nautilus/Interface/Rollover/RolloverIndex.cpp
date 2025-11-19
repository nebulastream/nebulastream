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

#include <Nautilus/Interface/Rollover/RolloverIndex.hpp>
#include <ErrorHandling.hpp>   // PRECONDITION, NES_WARNING etc. (if needed)

namespace NES::Nautilus::Interface
    {

        RolloverIndex::RolloverIndex(unsigned long rolloverValue, Callback onRollover)
            : index_(0)
            , rolloverValue_(rolloverValue)
            , callback_(onRollover)
        {
            PRECONDITION(rolloverValue_ > 0, "RolloverIndex: rolloverValue must be > 0");
        }

        void RolloverIndex::setIndex(unsigned long index)
        {
            index_ = index;
            if (index_ >= rolloverValue_)
            {
                rollover();
            }
        }

        unsigned long RolloverIndex::getIndex() const
        {
            return index_;
        }

        void RolloverIndex::setRolloverValue(unsigned long rolloverValue)
        {
            PRECONDITION(rolloverValue > 0, "RolloverIndex: rolloverValue must be > 0");

            // As per your notes: index remains unchanged when rollover value is updated.
            rolloverValue_ = rolloverValue;
        }

        unsigned long RolloverIndex::getRolloverValue() const
        {
            return rolloverValue_;
        }

        void RolloverIndex::increment()
        {
            ++index_;

            if (index_ >= rolloverValue_)
            {
                rollover();
            }
        }

        void RolloverIndex::rollover()
        {
            index_ = 0;

            if (callback_)
            {
                callback_();
            }
        }

    } // namespace NES::Nautilus::Interface
