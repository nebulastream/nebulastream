import React from 'react'
import {Button, Card, CardBody, CardHeader, Col, UncontrolledCollapse} from "reactstrap";
import Row from "reactstrap/es/Row";
import AceEditor from "react-ace";
import {Tree} from "react-d3-tree";
import 'brace/mode/c_cpp';
import 'brace/theme/github';
import ButtonGroup from "reactstrap/es/ButtonGroup";

export default class QueryInterface extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            data: [
                {
                    name: 'Top Level',
                    children: [
                        {
                            name: 'Level 2: A',
                        },
                        {
                            name: 'Level 2: B',
                        },
                    ],
                },
            ],
            myConfig: {
                maxZoom: 4,
                nodeHighlightBehavior: true,
                node: {
                    color: 'lightgreen',
                    size: 420,
                    highlightStrokeColor: 'blue'
                },
                link: {
                    highlightColor: 'lightblue',
                    renderLabel: true
                }
            },
        };
        this.svgSquare = {
            "shape": "rect",
            "shapeProps": {
                "width": 140,
                "height": 20,
                "y": -10,
                "x": -10
            }
        };
        this.userQuery = '';
        this.getQueryPlan = this.getQueryPlan.bind(this);
        this.updateQuery = this.updateQuery.bind(this);
        this.updateGraphData = this.updateGraphData.bind(this);
    }

    updateQuery(newQuery) {
        console.log(newQuery);
        this.userQuery = newQuery;
        console.log(this.userQuery);
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
                return response.json();
            })
            .then(data => {

                this.updateGraphData(data);
            })
            .catch(err => {
                console.log(err);
            });
        console.log("Fetching completed")
    }

    updateGraphData(jsonObject) {
        this.setState({data: jsonObject})
    }

    handleResponseError(response) {
        throw new Error("HTTP error, status = " + response.status);
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
                                <Row className="m-md-2">
                                    <Col className="mb-auto">
                                        <AceEditor
                                            mode="c_cpp"
                                            theme="github"
                                            fontSize={20}
                                            showPrintMargin={true}
                                            showGutter={true} l
                                            editorProps={{$blockScrolling: true}}
                                            setOptions={{
                                                showLineNumbers: true,
                                                tabSize: 2,
                                            }}
                                            name="query"
                                            onChange={this.updateQuery}
                                        />
                                    </Col>
                                </Row>
                                <Row className="m-md-2">
                                    <Col>
                                        <ButtonGroup>
                                            <Button color="primary">Query</Button>
                                            <Button color="primary" id="queryplan" onClick={() => {
                                                this.getQueryPlan(this.userQuery)
                                            }}>Query
                                                Plan</Button>
                                        </ButtonGroup>
                                    </Col>
                                </Row>
                                <Row className="m-md-2">
                                    <Col md="8">
                                        <UncontrolledCollapse toggler="#queryplan">
                                            <div style={{width: '50em', height: '30em'}}>
                                                <Tree
                                                    id="tree" // id is mandatory, if no id is defined rd3g will throw an error
                                                    data={this.state.data}
                                                    pathFunc='diagonal'
                                                    orientation='vertical'
                                                    nodeSvgShape={this.svgSquare}
                                                    separation={{siblings: 2, nonSiblings: 3}}
                                                    translate={{x: 200, y: 50}}
                                                    textLayout={{}}
                                                />
                                            </div>
                                        </UncontrolledCollapse>
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
