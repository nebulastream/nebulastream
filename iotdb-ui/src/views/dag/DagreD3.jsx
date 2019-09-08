import React from 'react'
import PropTypes from 'prop-types'
import * as dagreD3 from 'dagre-d3'
import * as d3 from 'd3'

import isEqual from 'react-fast-compare'

class DagreD3 extends React.Component {

    static defaultProps = {
        height: "1",
        width: "1",
        // width and height are defaulted to 1 due to a FireFox bug(?) If set to 0, it complains.
        fit: false,
        interactive: false
    };

    static propTypes = {
        nodes: PropTypes.object.isRequired,
        edges: PropTypes.array.isRequired,
        interactive: PropTypes.bool,
        fit: PropTypes.bool,
        height: PropTypes.string,
        width: PropTypes.string,
        shapeRenderers: PropTypes.objectOf(PropTypes.func),
        onNodeClick: PropTypes.func,
    };

    shouldComponentUpdate(nextProps, nextState) {
        return !isEqual(this.props.nodes, nextProps.nodes) ||
            !isEqual(this.props.edges, nextProps.edges) ||
            !isEqual(this.props.zoom, nextProps.zoom)
    }

    componentDidMount() {
        this.renderDag();
    }

    componentDidUpdate() {
        this.renderDag();
    }

    renderDag() {
        let g = new dagreD3.graphlib.Graph().setGraph({
            ranksep: 20, rankdir: "BT", acyclicer: "greedy"
        });


        for (let [id, node] of Object.entries(this.props.nodes)) {

            let defaultStyle = "fill: white;stroke: #000000; stroke-width: 1.5px;";
            let userDefined = node.style;
            node.style = defaultStyle + userDefined;
            g.setNode(id, node);
        }


        for (let edge of this.props.edges) {
            let edgeProp = {
                style: "stroke: blue; fill: #fff;stroke-width:2px;",
                arrowheadStyle: "fill: blue", ...edge[2]
            };
            g.setEdge(edge[0], edge[1], edgeProp); // from, to, props
        }

        // Set up an SVG group so that we can translate the final graph.
        let svg = d3.select(this.nodeTree);
        let inner = d3.select(this.nodeTreeGroup);

        // set up zoom support
        if (this.props.interactive) {
            let zoom = d3.zoom().on("zoom",
                () => inner.attr("transform", d3.event.transform));
            svg.call(zoom);
        }

        // Create the renderer
        let render = new dagreD3.render();

        // set up custom shape renderers
        if (this.props.shapeRenderers)
            for (let [shape, renderer] of Object.entries(this.props.shapeRenderers))
                render.shapes()[shape] = renderer;

        // Run the renderer. This is what draws the final graph.
        render(inner, g);


        // TODO add padding?
        if (this.props.fit) {
            let {height: gHeight, width: gWidth} = g.graph();
            let {height, width} = this.nodeTree.getBBox();
            let zoomScale = (width * height) / (gWidth * gHeight);
            let transX = ((width) - (gWidth * zoomScale)) / 2;
            let transY = ((height) - (gHeight * zoomScale)) / 2;
            svg.attr('height', height + 30);
            svg.attr('width', width + 40);
            inner.attr("transform", d3.zoomIdentity.translate(transX + 10, transY + 10))
        }

        //
        // let styleTooltip = function (name, description) {
        //     console.log(name + "-" + description)
        //     return "<p class='name'>" + name + "</p><p class='description'>" + description + "</p>";
        // };
        //
        // //Defining the tool tip section
        // svg.selectAll("g.node")
        //     .attr("title", function (v) {
        //         return styleTooltip(v, g.node(v).description)
        //     })
        //     .each(function (v) {
        //         console.log("ankit" + v);
        //         return (<Tipsy content="inside dagre"></Tipsy>);
        //     });

        if (this.props.onNodeClick)
            svg.selectAll('.dagre-d3 .node')
                .on('click', id => this.props.onNodeClick(id));


    }


    render() {
        return (
            <svg className='dagre-d3' ref={(r) => {
                this.nodeTree = r
            }}
                 width={this.props.width}
                 height={this.props.height}
            >

                <g ref={(r) => {
                    this.nodeTreeGroup = r
                }}
                />
            </svg>
        );
    }
}

export default DagreD3;