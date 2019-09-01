import React from 'react'
import {
    Alert,
    Button,
    Card,
    CardBody,
    CardHeader,
    Col,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Row
} from "reactstrap";
import AceEditor from "react-ace";
import 'brace/mode/c_cpp';
import 'brace/theme/github';
import ButtonGroup from "reactstrap/es/ButtonGroup";
import ButtonDropdown from "reactstrap/es/ButtonDropdown";
import {toast} from "react-toastify";
import {GraphView} from 'react-digraph';
import GraphConfig, {EMPTY_EDGE_TYPE, NODE_KEY, POLY_TYPE, SKINNY_TYPE,} from './dag/graph-config';
import DagreD3 from "./dag/DagreD3";

export default class QueryInterface extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            topologyGraph: {
                nodes: [],
                edges: []
            },
            queryGraph: {
                nodes: [],
                edges: []
            },
            executionGraph: {
                nodes: {

                    '6': {
                        label: 'A',
                        description: "represents state at all.",
                        style: "fill: #f77"
                    },
                    '1': {
                        label: 'Node 1',
                        style: "fill: #f47"
                    },
                    '2': {
                        label: 'Node 2',
                        style: "fill: #f57"
                    },
                    '3': {
                        label: 'Node 3',
                        style: "fill: #aff"
                    },
                    '4': {
                        label: 'Node 4',
                        description: "connection state at all.",
                        style: "fill: #f77"
                    },
                    '5': {
                        label: 'Node 5',
                        description: "represents no connectio at all.",
                        style: "fill: #f77"
                    }

                },
                edges: [
                    ['1', '3', {label: 'lalala', style:"stroke: #f77; fill: #fff;stroke-width:4px;",arrowheadStyle:"fill: #f77"}],
                    ['2', '3', {label: 'lalalal'}],
                    ['3', '5', {label: 'sd'}],
                    ['4', '5', {label: 'asdw'}],
                    ['5', '6', {label: 'asdw'}],
                ]
            },
            selectedStrategy: "NONE",
            displayBasePlan: false,
            displayExecutionPlan: false,
            displayTopologyPlan: false,
            openExecutionStrategy: false,
        };
        this.svgSquare = {
            "shape": "rect",
            "shapeProps": {
                "width": 110,
                "height": 20,
                "y": -10,
                "x": -10,
                fill: 'lightblue',
            }
        };
        this.userQuery = 'Schema schema = Schema::create()\n' +
            '   .addField("measurement",INT32);\n\n' +
            'Stream temperature = Stream("temperature", schema);\n\n' +
            'return InputQuery::from(temperature)\n' +
            '   .filter(temperature["measurement"] > 100)\n' +
            '   .print(std::cout);\n';
        this.getQueryPlan = this.getQueryPlan.bind(this);
        this.updateQuery = this.updateQuery.bind(this);
        this.updateData = this.updateData.bind(this);
        this.hideEverything = this.hideEverything.bind(this);
        this.resetTreeData = this.resetTreeData.bind(this);
        this.getExecutionPlan = this.getExecutionPlan.bind(this);
        this.toggleExecutionStrategy = this.toggleExecutionStrategy.bind(this);
        this.notify = this.notify.bind(this);
        this.generateDagFromJson = this.generateDagFromJson.bind(this);
        this.showNodeDescription=this.showNodeDescription.bind(this);
    }

    notify = (type, message) => {

        switch (type) {

            case "warn" :
                toast.warn(message, {
                    position: toast.POSITION.TOP_RIGHT,
                    autoClose: 8000,
                });
                break;
            case "err" :
                toast.error(message, {
                    position: toast.POSITION.TOP_RIGHT,
                    autoClose: 10000,
                });
                break;
            case "info" :
                toast.info(message, {
                    position: toast.POSITION.TOP_RIGHT,
                    autoClose: 5000,
                });
                break;
            default:
                toast(message);

        }
    };

    updateQuery(newQuery) {
        this.userQuery = newQuery;
    }

    getQueryPlan(userQuery) {
        fetch('http://127.0.0.1:8081/v1/iotdb/service/query-plan', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                },
                body: userQuery,
            }
        )
            .then(response => {
                if (!response.ok) {
                    this.handleResponseError(response);
                } else {
                    this.notify("info", "Fetched plan for user query")
                }
                return response.json();
            })
            .then(data => {
                this.updateData("query", data);
                this.setState({displayBasePlan: true});
            })
            .catch(err => {
                this.resetTreeData("query");
                if (err.message.includes("500")) {
                    this.notify("err", "Unable to fetch plan for input query!")
                } else if (err.message.includes("404")) {
                    this.notify("err", "Resource not found!")
                } else {
                    this.notify("err", err.message)
                }
                this.setState({displayBasePlan: false});
            });
    }

    getFogTopology() {
        fetch('http://127.0.0.1:8081/v1/iotdb/service/fog-plan', {
                method: 'GET',
                headers: {
                    Accept: 'application/json',
                }
            }
        )
            .then(response => {
                if (!response.ok) {
                    this.handleResponseError(response);
                } else {
                    this.notify("info", "Fetched fog topology graph")
                }
                return response.json();
            })
            .then(data => {
                this.updateData("topology", data);
                this.setState({displayTopologyPlan: true});
            })
            .catch(err => {
                this.resetTreeData("topology");
                if (err.message.includes("500")) {
                    this.notify("err", "Unable to fetch topology graph!")
                } else if (err.message.includes("404")) {
                    this.notify("err", "Resource not found!")
                } else {
                    this.notify("err", err.message)
                }
                this.setState({displayTopologyPlan: false});
            });
    }

    getExecutionPlan(userQuery, strategyName) {
        this.setState({selectedStrategy: strategyName});
        fetch('http://127.0.0.1:8081/v1/iotdb/service/execution-plan', {
                method: 'POST',
                headers: {
                    Accept: 'application/json',
                },
                body: JSON.stringify({
                    userQuery: userQuery,
                    strategyName: strategyName
                }),
            }
        )
            .then(response => {
                if (!response.ok) {
                    this.handleResponseError(response);
                } else {
                    this.notify("info", "Fetched execution plan for user query")
                }
                return response.json();
            })
            .then(data => {
                this.updateData("execution", data);
                this.setState({displayExecutionPlan: true});
            })
            .catch(err => {
                this.resetTreeData("execution");
                if (err.message.includes("500")) {
                    this.notify("err", "Unable to fetch execution plan for input query!")
                } else if (err.message.includes("404")) {
                    this.notify("err", "Resource not found!")
                } else {
                    this.notify("err", err.message)
                }
                this.setState({displayExecutionPlan: false});
            });
    }

    updateData(modelName, jsonObject) {
        let generatedSample = this.generateDagFromJson(jsonObject);
        if (modelName === 'query') {
            this.setState({queryGraph: generatedSample})
        } else if (modelName === 'topology') {
            this.setState({topologyGraph: generatedSample});
        } else if (modelName === 'execution') {
            this.setState({executionGraph: generatedSample})
        }
    }

    hideEverything() {
        this.setState({
            displayBasePlan: false,
            displayExecutionPlan: false,
            displayTopologyPlan: false,
        });
    }

    resetTreeData(modelName) {
        let generatedSample = {nodes: [], edges: []};
        if (modelName === 'query') {
            this.setState({queryGraph: generatedSample});
        } else if (modelName === 'topology') {
            this.setState({topologyGraph: generatedSample});
        } else if (modelName === 'execution') {
            this.setState({executionGraph: generatedSample});
        }
    }

    handleResponseError(response) {
        throw new Error("HTTP error, status = " + response.status);
    }

    toggleExecutionStrategy() {
        this.setState({
            openExecutionStrategy: !this.state.openExecutionStrategy
        });
    }

    showNodeDescription(id) {
        console.log(id);
    }

    generateDagFromJson(input) {

        let generatedSample = {nodes: [], edges: []};

        for (let i = 0; i < input.nodes.length; i++) {
            let inputNode = input.nodes[i];

            let type;
            if (inputNode.nodeType === "Sensor") {
                type = POLY_TYPE;
            } else {
                type = SKINNY_TYPE;
            }

            let node = {
                id: inputNode.id,
                title: inputNode.title,
                type: type,
                x: 10,
                y: 390,
            };
            generatedSample.nodes.push(node);
        }

        for (let i = 0; i < input.edges.length; i++) {
            let inputEdge = input.edges[i];

            let edge = {
                source: inputEdge.source,
                target: inputEdge.target,
                type: EMPTY_EDGE_TYPE,
            };
            generatedSample.edges.push(edge);
        }

        return generatedSample;
    };

    render() {
        const {NodeTypes, NodeSubtypes, EdgeTypes} = GraphConfig;

        return (
            <Col md="12">
                <Card>
                    <CardHeader>
                        <h1>Nebula Stream WebInterface</h1>
                    </CardHeader>
                    <CardBody>
                        <Alert className="m-md-2">Feed Your Query Here</Alert>
                        <Card className="m-md-2">
                            <AceEditor
                                focus={true}
                                mode="c_cpp"
                                theme="github"
                                fontSize={16}
                                width="100%"
                                height="300px"
                                showPrintMargin={true}
                                showGutter={true}
                                editorProps={{$blockScrolling: true}}
                                setOptions={{
                                    showLineNumbers: true,
                                    tabSize: 2,
                                }}
                                name="query"
                                onChange={this.updateQuery}
                                value={this.userQuery}
                            />
                        </Card>
                        <Row className="m-md-2">
                            <ButtonGroup>
                                <Button color="primary">Query</Button>
                                <Button color="primary" onClick={() => {
                                    this.getQueryPlan(this.userQuery)
                                }}>Show Query
                                    Plan</Button>
                                <Button color="primary" onClick={() => {
                                    this.getFogTopology()
                                }}>Show Fog
                                    Topology</Button>
                                <ButtonDropdown isOpen={this.state.openExecutionStrategy}
                                                toggle={this.toggleExecutionStrategy}>
                                    <DropdownToggle caret color="primary">Show Execution Plan</DropdownToggle>
                                    <DropdownMenu>
                                        <DropdownItem onClick={() => {
                                            this.getExecutionPlan(this.userQuery, "BottomUp")
                                        }}>Bottom-Up</DropdownItem>
                                        <DropdownItem onClick={() => {
                                            this.getExecutionPlan(this.userQuery, "TopDown")
                                        }}>Top-Down</DropdownItem>
                                    </DropdownMenu>
                                </ButtonDropdown>
                            </ButtonGroup>
                            <ButtonGroup>
                                <Button color="info" onClick={() => {
                                    this.hideEverything()
                                }}>Hide All</Button>
                            </ButtonGroup>
                        </Row>
                        <Row>
                            {this.state.displayBasePlan ?
                                <Col className="m-md-1" style={{width: '50%', height: '30em'}}>
                                    <div className="m-md-2 border" style={{width: '100%', height: '100%'}}>
                                        <Alert className="m-md-2">Query Plan</Alert>
                                        <div className="m-md-2" style={{height: '85%'}}>
                                            <GraphView
                                                nodes={this.state.queryGraph.nodes}
                                                edges={this.state.queryGraph.edges}
                                                nodeKey={NODE_KEY}
                                                nodeTypes={NodeTypes}
                                                nodeSubtypes={NodeSubtypes}
                                                edgeTypes={EdgeTypes}
                                                layoutEngineType={'VerticalTree'}
                                                gridDotSize={0}
                                            />
                                        </div>
                                    </div>
                                </Col> : null}

                            {this.state.displayTopologyPlan ?
                                <Col className="m-md-1" style={{width: '50%', height: '30em'}}>

                                    <div className="m-md-2 border" style={{width: '100%', height: '100%'}}>
                                        <Alert className="m-md-2">Fog Topology</Alert>
                                        <div className="m-md-2" style={{height: '85%'}}>
                                            <GraphView
                                                nodes={this.state.topologyGraph.nodes}
                                                edges={this.state.topologyGraph.edges}
                                                nodeKey={NODE_KEY}
                                                nodeTypes={NodeTypes}
                                                nodeSubtypes={NodeSubtypes}
                                                edgeTypes={EdgeTypes}
                                                layoutEngineType={'VerticalTree'}
                                                gridDotSize={0}
                                            />
                                        </div>
                                    </div>
                                </Col> : null}
                        </Row>
                        {this.state.displayExecutionPlan ?
                            <Row className="m-md-1" style={{width: '50%', height: '30em'}}>

                                <div className="m-md-2 border" style={{width: '100%', height: '100%'}}>
                                    <Alert className="m-md-2">Query execution plan for
                                        "{this.state.selectedStrategy}"
                                        strategy</Alert>
                                    <div className="m-md-2" style={{height: '85%'}}>
                                        <GraphView
                                            nodes={this.state.executionGraph.nodes}
                                            edges={this.state.executionGraph.edges}
                                            nodeKey={NODE_KEY}
                                            nodeTypes={NodeTypes}
                                            nodeSubtypes={NodeSubtypes}
                                            edgeTypes={EdgeTypes}
                                            layoutEngineType={'VerticalTree'}
                                            gridDotSize={0}
                                        />
                                    </div>
                                </div>
                            </Row> : null}
                    </CardBody>

                </Card>
                <div>
                    <DagreD3
                        edges={this.state.executionGraph.edges}
                        nodes={this.state.executionGraph.nodes}
                        interactive={false}
                        fit={true}
                    />
                </div>
            </Col>
        );
    }
};

