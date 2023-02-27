/*
 *      List.cpp
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
#include <Execution/StatisticsCollector/ChangeDetectors/List.hpp>

namespace NES::Runtime::Execution {

List::List(int maxBucketSize): maxBucketSize(maxBucketSize){
    head = NULL;
    tail = NULL;
    count = 0;
    addToHead();
}

List::~List(){
    while (head != NULL) removeFromHead();
}

void List::addToHead(){
    ListNode *temp = new ListNode(maxBucketSize);

    if (head) {
        temp->next = head;
        head->prev = temp;
    }
    head = temp;
    if (!tail)  tail = head;
    count++;
}

void List::addToTail(){
    ListNode *temp = new ListNode(maxBucketSize);
    if (tail) {
        temp->prev = tail;
        tail->next = temp;
    }
    tail = temp;
    if (!head) head = tail;
    count++;
}

void List::removeFromHead(){
    ListNode* temp;
    temp = head;
    head = head->next;
    if (head != NULL) {
        head->prev = NULL;
    } else {
        tail = NULL;
    }
    count--;
    delete temp;
}

void List::removeFromTail(){
    ListNode* temp;
    temp = tail;
    tail = tail->prev;
    if (tail == NULL)
        head = NULL;
    else
        tail->next=NULL;
    count--;
    delete temp;
}
} // namespace NES::Runtime::Execution