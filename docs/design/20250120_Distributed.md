# The Problem

The current system only supports queries across a single engine. The legacy system and NES however targets query processing
across geo distributed instances.

Designing a network stack is a non trivial task, especially targeting dynamically changing networks with nodes entering 
and leaving the system. 

The legacy system handled networking as an integral part to the system, there are countless references and special cases
checking for `NetworkSources` and `NetworkSinks`. The Legacy `DataSource` interface appears to missmatch the design of 
the `NetworkSource` and `NetworkSink`. ReconfigurationMessages have been extensively used to communicate if a source and
a sink is connected.

There are positive aspects about the Legacy Networking stack. The abstraction of using sources and sinks allow the
worker to be somewhat oblivious (disregarding the aforementioned problems) to if a query is distributed or not. Queries
where effectively chopped along a path of sources and sinks and connected via Network Source/Sink Pairs.

The user of the system could not decide how to deal with potential disconnects along the way, usually a disconnect ended
up crashing one node and then every other node. Buffering of data or general fault-tolerance in case of connection loss inbetween nodes was only partially implemented.

# Goals

- Enable Distributed Queries. Robust to not (yet) existing counterparts on different nodes.
- No modification to other components in the system. No special cases.
- Policy based propagation of faults. Potential infinite buffer of TupleBuffers if desired. 

# Non-Goals
- Optimizer and Placement.
- Coordination of queries. Buffering and waiting for counterparts should allow starting and stopping queries asynchronously

# (Optional) Solution Background
ZMQ
Single Node Queries


# Our Proposed Solution

We introduce a NetworkSource and NetworkSink into the system. These can be hidden behind the source provider and are thus
unknown to the rest of the system. They behave just like normal sources and sinks.

The actually chopping of the query plan into multiple connected query plans happens offline (e.g. in NebuLi or manually).
We can introduce a component that does that automatically for testing/demoing.

Ideal successor query is already running:
```mermaid
sequenceDiagram
box lime Upstream
    participant Op1 as Operator1
    participant s as Sink
end
box orange downstream
    participant S as Source
    participant Op2 as Operator2
end


    note over Op2: PipelineStarted
    note over S: SourceStarted
    activate Op2
    activate S

    note over Op1: PipelineStarted
    note over s: PipelineStarted
    activate Op1
    activate s
    s->>S: Establish connection
    S->>s: Connection established

    loop query is active
        Op1 ->> s: Data 
        s->>S: Data
        S->>Op2: Data
    end

    note over Op1: Pipeline Terminated
    deactivate Op1
    Op1 --) s: Terminate 
    note over s: Pipeline Terminated
    deactivate s

    s ->> S: EndOfStream

    note over S: Source Terminated
    deactivate S
    S --) Op2: Terminate 
    deactivate Op2
    note over Op2: Pipeline Terminated
```

Upstream Buffering
```mermaid
sequenceDiagram
box lime Upstream
    participant Op1 as Operator1
    participant s as Sink
end
box orange downstream
    participant S as Source
    participant Op2 as Operator2
end

    note over Op1, s: Query Started
    activate Op1
    activate s
    s->>S: Establish connection
    S--xs: Connection not successful

    loop query is active
        Op1 ->> s: Data 
        s->>S: Establish connection
        S--xs: Connection not successful
        s->>s: Buffer Data
    end


    note over S, Op2: Query Started
    activate Op2
    activate S
    
    loop query is active
        Op1 ->> s: Data 
        s->>S: Establish connection
        S->>s: Connection established
        loop Buffer
            s->>S: Data from Buffer
            S->>Op2: Data
        end
        s->>S: Data
        S->>Op2: Data
    end

    note over S, Op2: Query Terminated
    deactivate S
    S --) Op2: Terminate 
    deactivate Op2

    loop query is active
        Op1 ->> s: Data 
        s->>S: Data
        S--xs: Connection not sucessful
        s->>s: Buffer Data
    end

    deactivate s
    deactivate Op1
```

Downstream Buffering:
```mermaid
sequenceDiagram

box lime Upstream
    participant Op1 as Operator1
    participant s as Sink
    end

box Orange Downstream
    participant NM as NetworkManager
    participant S as Source
    participant Op2 as Operator2
end


    note over Op1, s: Query Started
    
    loop
        Op1 ->> s: Data
        s ->> NM: Data
        NM ->> NM: Buffer
    end

    note over Op2, S: Query Started

loop
    NM ->> S: Data
    S->> Op2: Data
end

loop
    Op1 ->> s: Data
    s ->> S: Data
    S->> Op2: Data
end
```

```mermaid
flowchart LR
    TCPSource --> Filter --> Project --> Join --> CSVSink
    TCPSource2 --> Filter2 --> Project2 --> Join
```

```mermaid
flowchart TD
    classDef net fill:#f96
    classDef ack fill:#4f6

    subgraph SinkNode 
        NSo2[NetworkSource]
        CSVSink2
        NSo2:::net --> CSVSink2

        NAck2[ACK]
        NSo2:::net -..-> NAck2:::ack
        
    end 

    subgraph IntermediateNode
        NSo1[NetworkSource]
        NSo11[NetworkSource]
        NSi1[NetworkSink]
        

        NSo11:::net ==> Join
        NSo1:::net ==> Join --> NSi1:::net
        

        NAck1[ACK]
        NAck11[ACK]
        Join -..-> NAck1:::ack
        Join -..-> NAck11:::ack
    end 

    NSi1 ==> NSo2

    subgraph InitialNode
        NSi0[NetworkSink]
        TCPSource ==> Filter ==> Project ==> NSi0:::net
    end

    subgraph InitialNode2
        NSi01[NetworkSink]
        TCPSource2 ==> Filter2 ==> Project2 ==> NSi01:::net
    end

    NSi0 ==> NSo1
    NSi01 ==> NSo11

    NAck1 -..-> NSi0
    NAck11 -..-> NSi01

    NAck2 -..-> NSi1
```

# Proof of Concept
- demonstrate that the solution should work
- can be done after the first draft

# Alternatives
- discuss alternative approaches A1, A2, ..., including their advantages and disadvantages

# (Optional) Open Questions
- list relevant questions that cannot or need not be answered before merging
- create issues if needed

# (Optional) Sources and Further Reading
- list links to resources that cover the topic

# (Optional) Appendix
- provide here nonessential information that could disturb the reading flow, e.g., implementation details
