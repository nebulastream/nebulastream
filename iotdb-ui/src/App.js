import React from 'react';
import { Breadcrumb, BreadcrumbItem, Alert } from 'reactstrap';

function App() {
  return (
      <div>
        <div>
          <Breadcrumb>
            <BreadcrumbItem active><h1>IotDB WebInterface</h1></BreadcrumbItem>
          </Breadcrumb>
        </div>
        <div>
          <Alert color="primary">
            This is a primary alert — check it out!
          </Alert>
        </div>

      </div>
  );
}

export default App;
