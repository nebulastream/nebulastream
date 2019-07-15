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
    Row,
    UncontrolledDropdown
} from "reactstrap";
import AceEditor from "react-ace";
import {Tree} from "react-d3-tree";
import 'brace/mode/c_cpp';
import 'brace/theme/github';
import ButtonGroup from "reactstrap/es/ButtonGroup";

export default class QueryInterface extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            queryPlan: [{"name": "Empty"}],
            executionPlan: [{"name": "Empty"}],
            topologyPlan: [{"name": "Empty"}],
            displayBasePlan: false,
            displayExecutionPlan: false,
            displayTopologyPlan: false,
            internalError: false,
        };
        this.svgSquare = {
            "shape": "rect",
            "shapeProps": {
                "width": 140,
                "height": 20,
                "y": -10,
                "x": -10,
                fill: 'olive',
            }
        };
        this.userQuery = 'Config config = Config::create()\n' +
            '   .setBufferCount(2000)\n' +
            '   .setBufferSizeInByte(8*1024)\n' +
            '   .setNumberOfWorker(2);\n' +
            'Schema schema = Schema::create()\n' +
            '   .addField("",INT32);\n' +
            'DataSourcePtr source = createTestSource();\n' +
            'return InputQuery::create(config, source)\n' +
            '   .filter(PredicatePtr())\n' +
            '   .print(std::cout);';
        this.getQueryPlan = this.getQueryPlan.bind(this);
        this.updateQuery = this.updateQuery.bind(this);
        this.updateData = this.updateData.bind(this);
        this.hideBasePlan = this.hideBasePlan.bind(this);
        this.resetTreeData = this.resetTreeData.bind(this);
        this.onDismiss = this.onDismiss.bind(this);
        this.showAlert = this.showAlert.bind(this);
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
                this.resetTreeData();
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
                this.resetTreeData();
                if (err.message.includes("500")) {
                    this.showAlert()
                } else {
                    console.log(err)
                }
            });
        this.setState({displayTopologyPlan: true});
        console.log("Fetching completed")
    }

    updateData(modelName, jsonObject) {
        if (modelName === 'query') {
            this.setState({queryPlan: jsonObject})
        } else if (modelName === 'topology') {
            this.setState({topologyPlan: jsonObject})
        }
    }

    hideBasePlan() {
        this.setState({displayBasePlan: false});
    }

    resetTreeData() {
        this.setState({queryPlan: [{"name": "empty"}]});
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

    render() {

        return (
            <>
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
                                        mode="c_cpp"
                                        theme="github"
                                        fontSize={16}
                                        width="100%"
                                        showPrintMargin={true}
                                        showGutter={true} l
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
                                    </ButtonGroup>
                                    <ButtonGroup>
                                        <Button color="info" onClick={() => {
                                            this.hideBasePlan()
                                        }}>Hide
                                            Plan</Button>
                                    </ButtonGroup>
                                </Row>
                                <Row className="m-md-1">
                                    <Col>
                                        <Row className="m-md-1 border">
                                            <Collapse isOpen={this.state.displayBasePlan}
                                                      style={{width: '30em', height: '30em'}}>
                                                <Tree
                                                    id="tree" // id is mandatory, if no id is defined rd3g will throw an error
                                                    data={this.state.queryPlan}
                                                    pathFunc='diagonal'
                                                    orientation='vertical'
                                                    nodeSvgShape={this.svgSquare}
                                                    separation={{siblings: 1, nonSiblings: 1}}
                                                    translate={{x: 200, y: 50}}
                                                    textLayout={{}}
                                                />
                                            </Collapse>
                                        </Row>
                                    </Col>
                                    <Col>
                                        <Row className="m-md-1 border">
                                            <Collapse isOpen={this.state.displayExecutionPlan}
                                                      style={{width: '30em', height: '30em'}}>
                                                <UncontrolledDropdown className="m-md-1">
                                                    <DropdownToggle caret>
                                                        Dropdown
                                                    </DropdownToggle>
                                                    <DropdownMenu>
                                                        <DropdownItem header>Header</DropdownItem>
                                                        <DropdownItem>Some Action</DropdownItem>
                                                        <DropdownItem disabled>Action (disabled)</DropdownItem>
                                                        <DropdownItem>Foo Action</DropdownItem>
                                                    </DropdownMenu>
                                                </UncontrolledDropdown>
                                                <Tree
                                                    id="tree" // id is mandatory, if no id is defined rd3g will throw an error
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
                                    </Col>
                                    <Col>
                                        <Row className="m-md-1 border">
                                            <Collapse isOpen={this.state.displayTopologyPlan}
                                                      style={{width: '30em', height: '30em'}}>
                                                <Tree
                                                    id="tree" // id is mandatory, if no id is defined rd3g will throw an error
                                                    data={this.state.topologyPlan}
                                                    pathFunc='diagonal'
                                                    orientation='vertical'
                                                    nodeSvgShape={this.svgSquare}
                                                    separation={{siblings: 1, nonSiblings: 1}}
                                                    translate={{x: 200, y: 50}}
                                                    textLayout={{}}
                                                />
                                            </Collapse>
                                        </Row>
                                    </Col>
                                </Row>
                            </CardBody>
                        </Card>
                    </Col>
                </div>
            </>
        );
    }
};
