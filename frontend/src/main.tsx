import React from 'react';
import ReactDOM from 'react-dom/client';
import { App } from './App';
import { IngestProvider } from './context/IngestContext';
import './index.css';

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <IngestProvider>
      <App />
    </IngestProvider>
  </React.StrictMode>
);
