import * as React from 'react';

import {
    GraphView
} from 'react-digraph';
import GraphConfig, {
    edgeTypes,
    EMPTY_EDGE_TYPE,
    EMPTY_TYPE,
    NODE_KEY,
    nodeTypes,
    POLY_TYPE,
    SPECIAL_CHILD_SUBTYPE,
    SPECIAL_EDGE_TYPE,
    SPECIAL_TYPE,
    SKINNY_TYPE,
} from './graph-config'; // Configures node/edge types

// NOTE: Edges must have 'source' & 'target' attributes
// In a more realistic use case, the graph would probably originate
// elsewhere in the App or be generated from some other state upstream of this component.
// const sample = {
//     edges: [
//         {
//             handleText: '5',
//             source: 'a1',
//             target: 'start1',
//             type: SPECIAL_EDGE_TYPE,
//         },
//         {
//             handleText: '5',
//             source: 'a1',
//             target: 'a2',
//             type: SPECIAL_EDGE_TYPE,
//         },
//         {
//             handleText: '54',
//             source: 'a2',
//             target: 'a4',
//             type: EMPTY_EDGE_TYPE,
//         },
//         {
//             handleText: '54',
//             source: 'a1',
//             target: 'a3',
//             type: EMPTY_EDGE_TYPE,
//         },
//         {
//             handleText: '54',
//             source: 'a3',
//             target: 'a4',
//             type: EMPTY_EDGE_TYPE,
//         },
//         {
//             handleText: '54',
//             source: 'a1',
//             target: 'a5',
//             type: EMPTY_EDGE_TYPE,
//         },
//         {
//             handleText: '54',
//             source: 'a4',
//             target: 'a1',
//             type: EMPTY_EDGE_TYPE,
//         },
//         {
//             handleText: '54',
//             source: 'a1',
//             target: 'a6',
//             type: EMPTY_EDGE_TYPE,
//         },
//         {
//             handleText: '24',
//             source: 'a1',
//             target: 'a7',
//             type: EMPTY_EDGE_TYPE,
//         },
//     ],
//     nodes: [
//         {
//             id: 'start1',
//             title: 'Start (0)',
//             type: SPECIAL_TYPE,
//         },
//         {
//             id: 'a1',
//             title: 'Node A (1)',
//             type: SPECIAL_TYPE,
//             x: 258.3976135253906,
//             y: 331.9783248901367,
//         },
//         {
//             id: 'a2',
//             subtype: EMPTY_TYPE,
//             title: 'Node B (2)',
//             type: SPECIAL_CHILD_SUBTYPE,
//             x: 593.9393920898438,
//             y: 260.6060791015625,
//         },
//         {
//             id: 'a3',
//             title: 'Node C (3)',
//             type: EMPTY_TYPE,
//             x: 237.5757598876953,
//             y: 61.81818389892578,
//         },
//         {
//             id: 'a4',
//             title: 'Node D (4)',
//             type: EMPTY_TYPE,
//             x: 600.5757598876953,
//             y: 600.81818389892578,
//         },
//         {
//             id: 'a5',
//             title: 'Node E (5)',
//             type: null,
//             x: 50.5757598876953,
//             y: 500.81818389892578,
//         },
//         {
//             id: 'a6',
//             title: 'Node E (6)',
//             type: SKINNY_TYPE,
//             x: 300,
//             y: 600,
//         },
//         {
//             id: 'a7',
//             title: 'Node F (7)',
//             type: POLY_TYPE,
//             x: 0,
//             y: 0,
//         },
//     ],
// };

const generatedSample = {
    edges: [],
    nodes: [],
};

class Graph extends React.Component<> {
    GraphView;


    constructor(props) {
        super(props);

        this.generateDagFromJson = this.generateDagFromJson.bind(this);
        this.generateDagFromJson("", 0, this.props.inputJson);

        this.state = {
            graph: generatedSample,
        };
        this.GraphView = React.createRef();
    }

    generateSample(totalNodes) {
        console.log("called");

        let y = 10;
        let x = 0;

        const numNodes = totalNodes ? totalNodes : 0;

        // generate large array of nodes
        // These loops are fast enough. 1000 nodes = .45ms + .34ms
        // 2000 nodes = .86ms + .68ms
        // implying a linear relationship with number of nodes.
        for (let i = 1; i <= numNodes; i++) {
            if (i % 20 === 0) {
                y++;
                x = 0;
            } else {
                x++;
            }

            generatedSample.nodes.push({
                id: `a${i}`,
                title: `Node ${i}`,
                type: nodeTypes[Math.floor(nodeTypes.length * Math.random())],
                x: 0 + 300 * x,
                y: 0 - 200 * y,
            });
        }
        // link each node to another node
        for (let i = 1; i < numNodes; i++) {
            generatedSample.edges.push({
                source: `a${i + 1}`,
                target: `a${i}`,
                type: edgeTypes[Math.floor(edgeTypes.length * Math.random())],
            });
        }

        return generatedSample;
    };

    generateDagFromJson(parentId, depth, input) {

        let x = 0;
        let y = depth;
        console.log(input.length);
        for (let i = 0; i < input.length; i++) {
            x = x + 200 * i;
            let currentId = input[i].id;
            console.log(currentId);
            console.log(x + "," + y);

            let node = {
                id: currentId,
                title: currentId,
                type: EMPTY_TYPE,
                x: x,
                y: y,
            };

            if (parentId !== "") {
                generatedSample.edges.push({
                    source: currentId,
                    target: parentId,
                    type: EMPTY_EDGE_TYPE,
                })
            }

            generatedSample.nodes.push(node);
            let children = input[i].children;
            if (typeof (children) !== "undefined") {
                this.generateDagFromJson(currentId, y + 300, children);
            }

            console.log("out now")
        }
    };

    /*
     * Render
     */

    render() {
        const {nodes, edges} = this.state.graph;
        const selected = this.state.selected;
        const {NodeTypes, NodeSubtypes, EdgeTypes} = GraphConfig;

        return (
            <GraphView
                ref={el => (this.GraphView = el)}
                nodeKey={NODE_KEY}
                nodes={nodes}
                edges={edges}
                selected={selected}
                nodeTypes={NodeTypes}
                nodeSubtypes={NodeSubtypes}
                edgeTypes={EdgeTypes}
                layoutEngineType={'VerticalTree'}
                primary={'red'}
            />
        );
    }
}

export default Graph;