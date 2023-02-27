/*
 *      ListNode.cpp
 *      
 *      Copyright (C) 2008 Universitat Politècnica de Catalunya
 *      @author Albert Bifet - Ricard Gavaldà (abifet,gavalda@lsi.upc.edu)
 *     
 *      This program is free software; you can redistribute it and/or modify
 *      it under the terms of the GNU General Public License as published by
 *      the Free Software Foundation; either version 2 of the License, or
 *      (at your option) any later version.
 *      
 *      This program is distributed in the hope that it will be useful,
 *      but WITHOUT ANY WARRANTY; without even the implied warranty of
 *      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *      GNU General Public License for more details.
 *      
 *      You should have received a copy of the GNU General Public License
 *      along with this program; if not, write to the Free Software
 *      Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
 *      MA 02110-1301, USA.
 */

#include <stdio.h>
#include <Execution/StatisticsCollector/ChangeDetectors/ListNode.hpp>

namespace NES::Runtime::Execution {

ListNode::ListNode(int maxSize):
    maxSize(maxSize),
    size(0),
    sum(maxSize + 1, 0.0),
    variance(maxSize + 1, 0.0),
    next(NULL),
    prev(NULL)
{}

void ListNode::addBack(const double &value, const double &var) {
    sum[size] = value;
    variance[size] = var;
    size++;
}

void ListNode::dropFront(int n) {
    //move n last elements to beginning of bucket
    for (int k = n; k <= maxSize; k++) {
        sum[k - n] = sum[k];
        variance[k - n] = variance[k];
    }

    // set last elements of bucket to 0
    for (int k = 1; k <= n; k++) {
        sum[maxSize - k + 1] = 0;
        variance[maxSize - k + 1] = 0;
    }

    size -= n;
}
} // namespace NES::Runtime::Execution