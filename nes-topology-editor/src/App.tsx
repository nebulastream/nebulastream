import { useEffect } from 'react';
import './App.css';
import '@xyflow/react/dist/style.css';
import './styles/nodes.css';
import { ReactFlowProvider } from '@xyflow/react';
import Canvas from './components/Canvas/Canvas';
import Toolbar from './components/Toolbar/Toolbar';
import QueryPanel from './components/QueryEditor/QueryPanel';
import Sidebar from './components/Sidebar/Sidebar';
import YamlOverlay from './components/YamlOverlay/YamlOverlay';
import StatusBar from './components/StatusBar/StatusBar';
import { useWasmValidation } from './hooks/useWasmValidation';
import { handleGlobalKeyDown } from './lib/keyboardHandler';

function App() {
  useEffect(() => {
    const handler = (e: KeyboardEvent) => handleGlobalKeyDown(e);
    window.addEventListener('keydown', handler);
    return () => window.removeEventListener('keydown', handler);
  }, []);

  // Start WASM validation worker lifecycle
  useWasmValidation();

  return (
    <ReactFlowProvider>
      <div className="app-container">
        <Toolbar />
        <div className="main-row">
          <div className="canvas-area" style={{ position: 'relative' }}>
            <Canvas />
            <YamlOverlay />
            <StatusBar />
          </div>
          <Sidebar />
        </div>
        <QueryPanel />
      </div>
    </ReactFlowProvider>
  );
}

export default App;
