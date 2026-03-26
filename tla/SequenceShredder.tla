--------------------------- MODULE SequenceShredder ---------------------------
(*
  MODEL OVERVIEW
  ==============
  This models the concurrent sequence range assignment problem in the
  NebulaStream InputFormatter / SequenceShredder.

  PHYSICAL SETUP
  A source reads raw data into fixed-size buffers (e.g. 16 bytes).
  Each buffer gets a sequence number: buffer i has range [i, i+1).
  Some buffers contain a record delimiter (e.g. '\n'), some don't.
  Records may span multiple buffers ("spanning tuples").

  SPANNING TUPLES
  A spanning tuple is a record whose bytes cross buffer boundaries.
  It is delimited by two delimiter-containing buffers and may include
  zero or more no-delimiter buffers in between.

  Example with 5 buffers (D=has delimiter, N=no delimiter):

      Buffer 1(D)   Buffer 2(D)   Buffer 3(D)   Buffer 4(N)   Buffer 5(D)
      [1,2)         [2,3)         [3,4)         [4,5)         [5,6)

  Spanning tuples form between consecutive delimiter buffers:
      ST_A = <<1, 2>>    (between delimiters in buffer 1 and 2)
      ST_B = <<2, 3>>    (between delimiters in buffer 2 and 3)
      ST_C = <<3, 5>>    (between delimiters in buffer 3 and 5, with buffer 4 in the middle)

  BUFFER TYPES
  - Delimiter buffer with complete tuples: has >= 2 delimiters, so it has
    records fully contained within it. Always emits output.
  - Delimiter buffer with 0 complete tuples: has exactly 1 delimiter.
    All its data belongs to spanning tuples. Only emits if it claims an ST.
  - No-delimiter buffer: has no delimiter at all. Its entire content is
    part of a spanning tuple. Only emits if it claims the ST.

  CONCURRENT CLAIMING (the CAS race)
  Multiple threads process buffers concurrently. Each spanning tuple must
  be processed by exactly one thread. The threads race to claim STs:

  - A delimiter buffer can claim its LEFT ST (as the right endpoint,
    called "leading" in the code) and its RIGHT ST (as the left endpoint,
    called "trailing" in the code).
  - A no-delimiter buffer can claim the ST that contains it.
  - Claiming is atomic (CAS): first thread to claim wins.

  EFFECTIVE RANGE
  After claiming, each buffer computes its effective output range:
  - Starts as [self, self+1) (the buffer's own sequence range)
  - Extended to include "middle" buffers (no-delimiter buffers between
    the two delimiter endpoints of a claimed ST)
  - The non-claiming endpoint of an ST is SKIPPED to avoid overlap
    (it will cover its own range or be discarded)

  DISCARD
  A buffer that claims nothing AND has 0 complete tuples produces 0
  output records. It is discarded (no output emitted). Its sequence
  range must be covered by a neighboring buffer that claimed an ST.

  INVARIANTS (checked when all buffers are done)
  1. NoOverlap:     No two emitting buffers have overlapping ranges
  2. NoCoverageGap: Every point in [1, NumBuffers+1) is covered by
                    some emitting buffer's range

  THE PROBLEM
  With the "skip non-claiming endpoint" rule and concurrent claiming,
  a delimiter buffer with 0 complete tuples can lose both ST races
  and be discarded, with neither claiming neighbor covering its range.
  This creates a gap.
*)

EXTENDS Integers, Sequences, FiniteSets, TLC

Min(S) == CHOOSE x \in S : \A y \in S : x <= y
Max(S) == CHOOSE x \in S : \A y \in S : x >= y

CONSTANTS
    NumBuffers,         \* Number of raw buffers (sequence numbers 1..NumBuffers)
    HasDelimiter,       \* HasDelimiter[i] = TRUE iff buffer i contains a record delimiter
    HasCompleteTuples   \* HasCompleteTuples[i] = TRUE iff buffer i has >= 1 record
                        \* fully contained within it (requires >= 2 delimiters)

ASSUME NumBuffers \in Nat \ {0}
ASSUME HasDelimiter \in [1..NumBuffers -> BOOLEAN]
ASSUME HasCompleteTuples \in [1..NumBuffers -> BOOLEAN]
ASSUME \A i \in 1..NumBuffers : HasCompleteTuples[i] => HasDelimiter[i]

\* ---------------------------------------------------------------------------
\* DERIVED TOPOLOGY: which spanning tuples exist and how buffers relate to them
\* ---------------------------------------------------------------------------

\* Set of buffer positions that contain a delimiter
DelimPos == {i \in 1..NumBuffers : HasDelimiter[i]}

\* Static next delimiter (ignoring availability) — used only for stClaimed domain
StaticNextDelim(p) == LET after == {d \in DelimPos : d > p}
                      IN IF after # {} THEN Min(after) ELSE 0

\* All possible STs based on static delimiter layout (independent of availability)
AllPossibleSTs == {<<d, StaticNextDelim(d)>> : d \in {d2 \in DelimPos : StaticNextDelim(d2) # 0}}

\* The "middle" buffers of an ST: everything between the two endpoints (exclusive)
\* These are always no-delimiter buffers whose entire content belongs to this ST.
MiddleBuffers(st) == {m \in 1..NumBuffers : st[1] < m /\ m < st[2]}



(* --algorithm SequenceShredder
\* Each buffer runs as a concurrent process with 4 steps:
\* 1. TryLeading:  delimiter buffers try to claim their left ST (CAS)
\* 2. TryTrailing: delimiter buffers try to claim their right ST (CAS)
\* 3. TryNoDelim:  no-delimiter buffers try to claim their containing ST (CAS)
\* 4. Finish:      compute effective range, decide emit or discard
\*
\* Steps are interleaved across all buffers (modeling concurrent execution).
\* The CAS is modeled as: check stClaimed[st] = 0, then atomically set it.

variables
    available = [i \in 1..NumBuffers |-> FALSE];
    \* stClaimed[st] = 0 means unclaimed; otherwise the buffer id that claimed it
    stClaimed = [st \in AllPossibleSTs |-> 0];
    \* Output range for each buffer (0,0 if discarded)
    eStart = [b \in 1..NumBuffers |-> b];
    eEnd = [b \in 1..NumBuffers |-> b + 1];
    \* Whether this buffer will produce output
    emitting = [b \in 1..NumBuffers |-> FALSE];
    \* Whether this buffer finished processing
    done = [b \in 1..NumBuffers |-> FALSE];

define
    \* Are all buffer available from p to q
    AllAvailable(p, q) == \A d \in p..q: available[d]

    \* Nearest delimiter buffer AFTER position p (0 if none)
    NextDelim(p) == LET after == {d \in DelimPos : d > p /\ AllAvailable(p, d)}
                    IN IF after # {} THEN Min(after) ELSE 0

    \* Nearest delimiter buffer BEFORE position p (0 if none)
    PrevDelim(p) == LET before == {d \in DelimPos : d < p /\ AllAvailable(p, d)}
                    IN IF before # {} THEN Max(before) ELSE 0

    \* All spanning tuples, identified as <<leftDelimBuffer, rightDelimBuffer>>
    SpanningTuples == {<<d, NextDelim(d)>> : d \in {d2 \in DelimPos : NextDelim(d2) # 0}}

    \* The ST to the LEFT of delimiter buffer b (where b is the right endpoint)
    \* This is what the code calls the "leading" spanning tuple
    LeftST(b) == IF PrevDelim(b) # 0 THEN <<PrevDelim(b), b>> ELSE <<0, 0>>

    \* The ST to the RIGHT of delimiter buffer b (where b is the left endpoint)
    \* This is what the code calls the "trailing" spanning tuple
    RightST(b) == IF NextDelim(b) # 0 THEN <<b, NextDelim(b)>> ELSE <<0, 0>>

    \* The ST that CONTAINS a no-delimiter buffer (it sits between two delimiters)
    ContainingST(b) == IF PrevDelim(b) # 0 /\ NextDelim(b) # 0
                       THEN <<PrevDelim(b), NextDelim(b)>>
                       ELSE <<0, 0>>

    \* The set of buffers that produce output
    EmittingSet == {b \in 1..NumBuffers : emitting[b]}

    \* True when all buffers finished
    AllDone == \A b \in 1..NumBuffers : done[b]

    \* INVARIANT: no two emitting buffers have overlapping ranges
    NoOverlap == \A b1, b2 \in EmittingSet :
        b1 # b2 => (eEnd[b1] <= eStart[b2] \/ eEnd[b2] <= eStart[b1])

    \* INVARIANT: every sequence number 1..NumBuffers is covered by some emitting buffer
    NoCoverageGap == \A p \in 1..NumBuffers :
        \E b \in EmittingSet : eStart[b] <= p /\ p < eEnd[b]

    \* Combined: only checked when all buffers are done
    RangeInvariant == AllDone => (NoOverlap /\ NoCoverageGap)

    \* ---------------------------------------------------------------------------
    \* EFFECTIVE RANGE COMPUTATION
    \* ---------------------------------------------------------------------------
    \* Given which STs buffer b claimed, compute the output range [start, end).
    \* Strategy: include the buffer's own range + middle buffers of claimed STs.
    \* The non-claiming endpoint is skipped (to avoid overlap).
    EffRange(b, leading, trailing, nodelim) ==
        LET lst == LeftST(b)
            rst == RightST(b)
            cst == ContainingST(b)
            \* Leading ST [A, ..., B] claimed by B=self: include middle buffers, skip A
            lm == IF leading /\ lst \in SpanningTuples THEN MiddleBuffers(lst) ELSE {}
            \* Trailing ST [A, ..., B] claimed by A=self: include middle buffers, skip B
            rm == IF trailing /\ rst \in SpanningTuples THEN MiddleBuffers(rst) ELSE {}
            \* No-delim ST [A, ..., B] claimed by middle buffer self: skip both A and B
            nm == IF nodelim /\ cst \in SpanningTuples THEN MiddleBuffers(cst) ELSE {}
            all == {b} \cup lm \cup rm \cup nm
        IN <<Min(all), Max(all) + 1>>
    end define;

fair process buf \in 1..NumBuffers
variables
    cl = FALSE;   \* TRUE if this buffer claimed its left (leading) ST
    ct = FALSE;   \* TRUE if this buffer claimed its right (trailing) ST
    cn = FALSE;   \* TRUE if this buffer claimed its containing ST (no-delim only)
begin
    InsertBuffer:
        available[self] := TRUE;

    \* Step 1: Delimiter buffers try to claim the ST on their LEFT side
    \* (In the code: findLeadingSpanningTupleWithDelimiter)
    TryLeading:
        if HasDelimiter[self] /\ LeftST(self) \in SpanningTuples /\ stClaimed[LeftST(self)] = 0 then
            stClaimed[LeftST(self)] := self;
            cl := TRUE;
        end if;

    \* Step 2: Delimiter buffers try to claim the ST on their RIGHT side
    \* (In the code: findTrailingSpanningTupleWithDelimiter)
    TryTrailing:
        if HasDelimiter[self] /\ RightST(self) \in SpanningTuples /\ stClaimed[RightST(self)] = 0 then
            stClaimed[RightST(self)] := self;
            ct := TRUE;
        end if;

    \* Step 3: No-delimiter buffers try to claim the ST that contains them
    \* (In the code: findSpanningTupleWithoutDelimiter)
    TryNoDelim:
        if ~HasDelimiter[self] /\ ContainingST(self) \in SpanningTuples /\ stClaimed[ContainingST(self)] = 0 then
            stClaimed[ContainingST(self)] := self;
            cn := TRUE;
        end if;

    \* Step 4: Compute effective range and emit or discard
    Finish:
        if HasCompleteTuples[self] \/ cl \/ ct \/ cn then
            \* This buffer has output: complete tuples or claimed spanning tuple(s)
            eStart[self] := EffRange(self, cl, ct, cn)[1];
            eEnd[self] := EffRange(self, cl, ct, cn)[2];
            emitting[self] := TRUE;
            done[self] := TRUE;
        else
            \* DISCARD: no complete tuples and claimed nothing
            eStart[self] := 0;
            eEnd[self] := 0;
            emitting[self] := FALSE;
            done[self] := TRUE;
        end if;
end process;

end algorithm; *)

\* BEGIN TRANSLATION
VARIABLES available, stClaimed, eStart, eEnd, emitting, done, pc

(* define statement *)
AllAvailable(p, q) == \A d \in p..q: available[d]


NextDelim(p) == LET after == {d \in DelimPos : d > p /\ AllAvailable(p, d)}
                IN IF after # {} THEN Min(after) ELSE 0


PrevDelim(p) == LET before == {d \in DelimPos : d < p /\ AllAvailable(p, d)}
                IN IF before # {} THEN Max(before) ELSE 0


SpanningTuples == {<<d, NextDelim(d)>> : d \in {d2 \in DelimPos : NextDelim(d2) # 0}}



LeftST(b) == IF PrevDelim(b) # 0 THEN <<PrevDelim(b), b>> ELSE <<0, 0>>



RightST(b) == IF NextDelim(b) # 0 THEN <<b, NextDelim(b)>> ELSE <<0, 0>>


ContainingST(b) == IF PrevDelim(b) # 0 /\ NextDelim(b) # 0
                   THEN <<PrevDelim(b), NextDelim(b)>>
                   ELSE <<0, 0>>


EmittingSet == {b \in 1..NumBuffers : emitting[b]}


AllDone == \A b \in 1..NumBuffers : done[b]


NoOverlap == \A b1, b2 \in EmittingSet :
    b1 # b2 => (eEnd[b1] <= eStart[b2] \/ eEnd[b2] <= eStart[b1])


NoCoverageGap == \A p \in 1..NumBuffers :
    \E b \in EmittingSet : eStart[b] <= p /\ p < eEnd[b]


RangeInvariant == AllDone => (NoOverlap /\ NoCoverageGap)







EffRange(b, leading, trailing, nodelim) ==
    LET lst == LeftST(b)
        rst == RightST(b)
        cst == ContainingST(b)

        lm == IF leading /\ lst \in SpanningTuples THEN MiddleBuffers(lst) ELSE {}

        rm == IF trailing /\ rst \in SpanningTuples THEN MiddleBuffers(rst) ELSE {}

        nm == IF nodelim /\ cst \in SpanningTuples THEN MiddleBuffers(cst) ELSE {}
        all == {b} \cup lm \cup rm \cup nm
    IN <<Min(all), Max(all) + 1>>

VARIABLES cl, ct, cn

vars == << available, stClaimed, eStart, eEnd, emitting, done, pc, cl, ct, cn
        >>

ProcSet == (1..NumBuffers)

Init == (* Global variables *)
        /\ available = [i \in 1..NumBuffers |-> FALSE]
        /\ stClaimed = [st \in AllPossibleSTs |-> 0]
        /\ eStart = [b \in 1..NumBuffers |-> b]
        /\ eEnd = [b \in 1..NumBuffers |-> b + 1]
        /\ emitting = [b \in 1..NumBuffers |-> FALSE]
        /\ done = [b \in 1..NumBuffers |-> FALSE]
        (* Process buf *)
        /\ cl = [self \in 1..NumBuffers |-> FALSE]
        /\ ct = [self \in 1..NumBuffers |-> FALSE]
        /\ cn = [self \in 1..NumBuffers |-> FALSE]
        /\ pc = [self \in ProcSet |-> "InsertBuffer"]

InsertBuffer(self) == /\ pc[self] = "InsertBuffer"
                      /\ available' = [available EXCEPT ![self] = TRUE]
                      /\ pc' = [pc EXCEPT ![self] = "TryLeading"]
                      /\ UNCHANGED << stClaimed, eStart, eEnd, emitting, done, 
                                      cl, ct, cn >>

TryLeading(self) == /\ pc[self] = "TryLeading"
                    /\ IF HasDelimiter[self] /\ LeftST(self) \in SpanningTuples /\ stClaimed[LeftST(self)] = 0
                          THEN /\ stClaimed' = [stClaimed EXCEPT ![LeftST(self)] = self]
                               /\ cl' = [cl EXCEPT ![self] = TRUE]
                          ELSE /\ TRUE
                               /\ UNCHANGED << stClaimed, cl >>
                    /\ pc' = [pc EXCEPT ![self] = "TryTrailing"]
                    /\ UNCHANGED << available, eStart, eEnd, emitting, done, 
                                    ct, cn >>

TryTrailing(self) == /\ pc[self] = "TryTrailing"
                     /\ IF HasDelimiter[self] /\ RightST(self) \in SpanningTuples /\ stClaimed[RightST(self)] = 0
                           THEN /\ stClaimed' = [stClaimed EXCEPT ![RightST(self)] = self]
                                /\ ct' = [ct EXCEPT ![self] = TRUE]
                           ELSE /\ TRUE
                                /\ UNCHANGED << stClaimed, ct >>
                     /\ pc' = [pc EXCEPT ![self] = "TryNoDelim"]
                     /\ UNCHANGED << available, eStart, eEnd, emitting, done, 
                                     cl, cn >>

TryNoDelim(self) == /\ pc[self] = "TryNoDelim"
                    /\ IF ~HasDelimiter[self] /\ ContainingST(self) \in SpanningTuples /\ stClaimed[ContainingST(self)] = 0
                          THEN /\ stClaimed' = [stClaimed EXCEPT ![ContainingST(self)] = self]
                               /\ cn' = [cn EXCEPT ![self] = TRUE]
                          ELSE /\ TRUE
                               /\ UNCHANGED << stClaimed, cn >>
                    /\ pc' = [pc EXCEPT ![self] = "Finish"]
                    /\ UNCHANGED << available, eStart, eEnd, emitting, done, 
                                    cl, ct >>

Finish(self) == /\ pc[self] = "Finish"
                /\ IF HasCompleteTuples[self] \/ cl[self] \/ ct[self] \/ cn[self]
                      THEN /\ eStart' = [eStart EXCEPT ![self] = EffRange(self, cl[self], ct[self], cn[self])[1]]
                           /\ eEnd' = [eEnd EXCEPT ![self] = EffRange(self, cl[self], ct[self], cn[self])[2]]
                           /\ emitting' = [emitting EXCEPT ![self] = TRUE]
                           /\ done' = [done EXCEPT ![self] = TRUE]
                      ELSE /\ eStart' = [eStart EXCEPT ![self] = 0]
                           /\ eEnd' = [eEnd EXCEPT ![self] = 0]
                           /\ emitting' = [emitting EXCEPT ![self] = FALSE]
                           /\ done' = [done EXCEPT ![self] = TRUE]
                /\ pc' = [pc EXCEPT ![self] = "Done"]
                /\ UNCHANGED << available, stClaimed, cl, ct, cn >>

buf(self) == InsertBuffer(self) \/ TryLeading(self) \/ TryTrailing(self)
                \/ TryNoDelim(self) \/ Finish(self)

(* Allow infinite stuttering to prevent deadlock on termination. *)
Terminating == /\ \A self \in ProcSet: pc[self] = "Done"
               /\ UNCHANGED vars

Next == (\E self \in 1..NumBuffers: buf(self))
           \/ Terminating

Spec == /\ Init /\ [][Next]_vars
        /\ \A self \in 1..NumBuffers : WF_vars(buf(self))

Termination == <>(\A self \in ProcSet: pc[self] = "Done")

\* END TRANSLATION

\* ===========================================================================
\* MODEL CONFIGURATION
\* ===========================================================================
\* Buffer pattern: D(multi), D(multi), D*(1 delim, 0 complete), N, D(multi)
\*
\*   Buf 1: delimiter, has complete tuples (>= 2 delimiters)
\*   Buf 2: delimiter, has complete tuples
\*   Buf 3: delimiter, 0 complete tuples (exactly 1 delimiter)  <-- problematic
\*   Buf 4: no delimiter
\*   Buf 5: delimiter, has complete tuples
\*
\* STs: <<1,2>>, <<2,3>>, <<3,5>>
\* Buffer 3 participates in ST <<2,3>> (right endpoint) and ST <<3,5>> (left endpoint)
\* If buffer 3 loses both CAS races, it has 0 tuples and is discarded.
HasDelimiterDef == [i \in 1..NumBuffers |-> i \in {1, 2, 3, 5}]
HasCompleteTuplesDef == [i \in 1..NumBuffers |-> i \in {1, 2, 5}]

=============================================================================
