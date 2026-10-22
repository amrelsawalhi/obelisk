import React from 'react';
import ReactDOM from 'react-dom/client';
import { BrowserRouter } from 'react-router-dom';
import { App } from './App';
import { IngestProvider } from './context/IngestContext';
import './index.css';

ReactDOM.createRoot(document.getElementById('root') as HTMLElement).render(
  <React.StrictMode>
    <BrowserRouter>
      <IngestProvider>
        <App />
      </IngestProvider>
    </BrowserRouter>
  </React.StrictMode>
);
