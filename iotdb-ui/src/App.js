import React from 'react';

import QueryInterface from "./views/QueryInterface";
import {Slide, ToastContainer} from "react-toastify";
import 'react-toastify/dist/ReactToastify.css';

class App extends React.Component {
    render() {
        return (
            <div>
                <ToastContainer position="top-right"
                                hideProgressBar={true}
                                newestOnTop={true}
                                closeOnClick = {true}
                                rtl={false}
                                pauseOnVisibilityChange = {true}
                                draggable = {false}
                                pauseOnHover = {true}
                                transition = {Slide}
                />
                <QueryInterface/>
            </div>
        );

    }

}

export default App;
