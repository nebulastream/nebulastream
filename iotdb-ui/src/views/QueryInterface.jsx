import React from 'react'
import {Button, Card, CardBody, CardHeader, Col, UncontrolledCollapse} from "reactstrap";
import Row from "reactstrap/es/Row";
import AceEditor from "react-ace";
import {Graph} from "react-d3-graph";
import 'brace/mode/c_cpp';
import 'brace/theme/github';

export default class QueryInterface extends React.Component {

    data = {
        nodes: [{id: 'Harry'}, {id: 'Sally'}, {id: 'Alice'}],
        links: [{source: 'Harry', target: 'Sally', label: "son"}, {source: 'Harry', target: 'Alice', label: "brother"}]
    };

    myConfig = {
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
    };

    getQueryPlan() {

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
                                            width="800px"
                                            fontSize={20}
                                            showPrintMargin={true}
                                            showGutter={true} l
                                            editorProps={{$blockScrolling: true}}
                                            setOptions={{
                                                showLineNumbers: true,
                                                tabSize: 2,
                                            }}
                                        />
                                    </Col>
                                </Row>
                                <Row className="m-md-2">
                                    <Col>
                                        <Button color="primary">Query</Button>{' '}
                                        <Button color="primary" id="queryplan">Query Plan</Button>
                                    </Col>
                                </Row>
                                <Row className="m-md-2">
                                    <Col md="8">
                                        <UncontrolledCollapse toggler="#queryplan">
                                            <Card>
                                                <CardBody>
                                                    <Graph
                                                        id="graph-id" // id is mandatory, if no id is defined rd3g will throw an error
                                                        data={this.data}
                                                        config={this.myConfig}
                                                    />
                                                </CardBody>
                                            </Card>
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
