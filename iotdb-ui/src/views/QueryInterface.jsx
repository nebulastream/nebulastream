import React from 'react'
import {Alert, Button, Card, CardBody, CardHeader, Col, Collapse, Row} from "reactstrap";
import AceEditor from "react-ace";
import {Tree} from "react-d3-tree";
import 'brace/mode/c_cpp';
import 'brace/theme/monokai';
import ButtonGroup from "reactstrap/es/ButtonGroup";

export default class QueryInterface extends React.Component {

    constructor(props, context) {
        super(props, context);
        this.state = {
            data: [{"name": "Empty"}],
            displayBasePlan: false,
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
        this.updateGraphData = this.updateGraphData.bind(this);
        this.hideBasePlan = this.hideBasePlan.bind(this);
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
        this.setState({displayBasePlan: true});
        console.log("Fetching completed")
    }

    updateGraphData(jsonObject) {
        this.setState({data: jsonObject})
    }

    hideBasePlan() {
        this.setState({displayBasePlan: false});
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
                                <Alert>Feed Your Query Here</Alert>
                                <Row className="m-md-2">
                                    <Col className="mb-auto">
                                        <AceEditor
                                            mode="c_cpp"
                                            theme="monokai"
                                            fontSize={16}
                                            width='50em'
                                            height='30em'
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
                                    </Col>
                                </Row>
                                <Row className="m-md-2">
                                    <Col>
                                        <ButtonGroup>
                                            <Button color="primary">Query</Button>
                                            <Button color="primary" onClick={() => {
                                                this.getQueryPlan(this.userQuery)
                                            }}>Query
                                                Plan</Button>
                                        </ButtonGroup>
                                        {' '}
                                        <ButtonGroup>
                                            <Button color="info" onClick={() => {
                                                this.hideBasePlan()
                                            }}>Hide
                                                Plan</Button>
                                        </ButtonGroup>
                                    </Col>
                                </Row>
                                <Row className="m-md-2">
                                    <Col md="8">
                                        <Collapse isOpen={this.state.displayBasePlan} toggler="#queryplan">
                                            <div style={{width: '50em', height: '30em'}}>
                                                <Tree
                                                    id="tree" // id is mandatory, if no id is defined rd3g will throw an error
                                                    data={this.state.data}
                                                    pathFunc='diagonal'
                                                    orientation='vertical'
                                                    nodeSvgShape={this.svgSquare}
                                                    separation={{siblings: 1, nonSiblings: 1}}
                                                    translate={{x: 200, y: 50}}
                                                    textLayout={{}}
                                                    styles={{fill: 'red'}}
                                                />
                                            </div>
                                        </Collapse>
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
