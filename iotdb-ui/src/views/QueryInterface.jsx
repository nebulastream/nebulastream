import React from 'react'
import {
    Alert,
    Button,
    Card,
    CardBody,
    CardHeader,
    Col,
    Collapse,
    DropdownItem,
    DropdownMenu,
    DropdownToggle,
    Row
} from "reactstrap";
import AceEditor from "react-ace";
import {Tree} from "react-d3-tree";
import 'brace/mode/c_cpp';
import 'brace/theme/github';
import ButtonGroup from "reactstrap/es/ButtonGroup";
import ButtonDropdown from "reactstrap/es/ButtonDropdown";

export default class QueryInterface extends React.Component {


    constructor(props, context) {
        super(props, context);
        this.state = {
            queryPlan: [{"name": "Empty"}],
            executionPlan: [{"name": "Empty"}],
            topologyPlan: [{"name": "Empty"}],
            selectedStrategy: "NONE",
            displayBasePlan: false,
            displayExecutionPlan: false,
            displayTopologyPlan: false,
            internalError: false,
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
        this.onDismiss = this.onDismiss.bind(this);
        this.showAlert = this.showAlert.bind(this);
        this.getExecutionPlan = this.getExecutionPlan.bind(this);
        this.toggleExecutionStrategy = this.toggleExecutionStrategy.bind(this);
    }

    updateQuery(newQuery) {
        this.userQuery = newQuery;
    }

    getQueryPlan(userQuery) {
        console.log("Fetching query plan");
        console.log(userQuery);
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
                }
                this.onDismiss();
                return response.json();
            })
            .then(data => {

                this.updateData("query", data);
            })
            .catch(err => {
                this.resetTreeData("query");
                if (err.message.includes("500")) {
                    this.showAlert()
                } else {
                    console.log(err)
                }
            });
        this.setState({displayBasePlan: true});
        console.log("Fetching completed")
    }

    getFogTopology() {
        console.log("Fetching Fog Topology");
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
                }
                this.onDismiss();
                return response.json();
            })
            .then(data => {
                this.updateData("topology", data);
            })
            .catch(err => {
                this.resetTreeData("topology");
                if (err.message.includes("500")) {
                    this.showAlert()
                } else {
                    console.log(err)
                }
            });
        this.setState({displayTopologyPlan: true});
        console.log("Fetching completed")
    }

    getExecutionPlan(userQuery, strategyName) {
        this.setState({selectedStrategy: strategyName});
        this.setState({displayExecutionPlan: true});
        console.log("Fetching query plan");
        console.log(userQuery);
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
                }
                this.onDismiss();
                return response.json();
            })
            .then(data => {

                this.updateData("execution", data);
            })
            .catch(err => {
                this.resetTreeData("execution");
                if (err.message.includes("500")) {
                    this.showAlert()
                } else {
                    console.log(err)
                }
            });
        console.log("Fetching completed")
    }

    updateData(modelName, jsonObject) {
        if (modelName === 'query') {
            this.setState({queryPlan: jsonObject})
        } else if (modelName === 'topology') {
            this.setState({topologyPlan: jsonObject})
        } else if (modelName === 'execution') {
            this.setState({executionPlan: jsonObject})
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
        if (modelName === 'query') {
            this.setState({queryPlan: [{"name": "empty"}]});
        } else if (modelName === 'topology') {
            this.setState({topologyPlan: [{"name": "empty"}]});
        } else if (modelName === 'execution') {
            this.setState({executionPlan: [{"name": "empty"}]});
        }
    }

    handleResponseError(response) {
        throw new Error("HTTP error, status = " + response.status);
    }

    showAlert() {
        this.setState({internalError: true});
    }

    onDismiss() {
        this.setState({internalError: false});
    }

    toggleExecutionStrategy() {
        this.setState({
            openExecutionStrategy: !this.state.openExecutionStrategy
        });
    }

    render() {

        return (
            <div>
                <Col md="12">
                    <Card>
                        <CardHeader>
                            <h1>IotDB WebInterface</h1>
                        </CardHeader>
                        <CardBody>
                            <Alert className="m-md-2">Feed Your Query Here</Alert>
                            <Alert className="m-md-2" color="danger" isOpen={this.state.internalError}
                                   toggle={this.onDismiss}
                                   fade={true}>
                                Received internal server error for user query. Please make sure the input query is
                                correct.
                            </Alert>
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
                                        <Button id="caret" color="primary">Show Execution Plan</Button>
                                        <DropdownToggle caret color="primary"/>
                                        <DropdownMenu>
                                            <DropdownItem onClick={() => {
                                                this.getExecutionPlan(this.userQuery, "BottomUp")
                                            }}>Bottom-Up</DropdownItem>
                                        </DropdownMenu>
                                    </ButtonDropdown>
                                </ButtonGroup>
                                <ButtonGroup>
                                    <Button color="info" onClick={() => {
                                        this.hideEverything()
                                    }}>Hide All</Button>
                                </ButtonGroup>
                            </Row>
                            <Row className="m-md-2">
                                <Collapse isOpen={this.state.displayBasePlan}
                                          style={{width: '100%', height: '30em'}} className="border">
                                    <Alert className="m-md-2">Query Plan</Alert>
                                    <Tree
                                        id="queryPlanTree"
                                        data={this.state.queryPlan}
                                        pathFunc='diagonal'
                                        orientation='vertical'
                                        nodeSvgShape={this.svgSquare}
                                        separation={{siblings: 1, nonSiblings: 1}}
                                        translate={{x: 400, y: 50}}
                                        textLayout={{}}
                                    />
                                </Collapse>
                            </Row>
                            <Row className="m-md-2">
                                <Collapse isOpen={this.state.displayExecutionPlan}
                                          style={{width: '100%', height: '30em'}} className="border">
                                    <Alert className="m-md-2">Query execution plan for "{this.state.selectedStrategy}"
                                        strategy</Alert>
                                    <Tree
                                        id="queryExecutionPlanTree"
                                        data={this.state.executionPlan}
                                        pathFunc='diagonal'
                                        orientation='vertical'
                                        nodeSvgShape={this.svgSquare}
                                        separation={{siblings: 1, nonSiblings: 1}}
                                        translate={{x: 200, y: 50}}
                                        textLayout={{}}
                                    />
                                </Collapse>
                            </Row>
                            <Row className="m-md-2">
                                <Collapse isOpen={this.state.displayTopologyPlan}
                                          style={{width: '100%', height: '30em'}} className="border">
                                    <Alert className="m-md-2">Fog Topology</Alert>
                                    <Tree
                                        id="fogTopologyTree"
                                        data={this.state.topologyPlan}
                                        pathFunc='diagonal'
                                        orientation='vertical'
                                        nodeSvgShape={this.svgSquare}
                                        separation={{siblings: 1, nonSiblings: 1}}
                                        translate={{x: 400, y: 50}}
                                        textLayout={{}}
                                    />
                                </Collapse>
                            </Row>
                        </CardBody>
                    </Card>
                </Col>
            </div>
        );
    }
};
