import React from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import DataPlatform from './pages/DataPlatform';

// Create a client
const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5, // 5 minutes
      retry: 1,
    },
  },
});

function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <Routes>
          <Route path="/" element={<DataPlatform />} />
          <Route path="/ai" element={<DataPlatform />} />
          <Route path="/ingestion" element={<DataPlatform />} />
          <Route path="/transform" element={<DataPlatform />} />
          <Route path="/query" element={<DataPlatform />} />
        </Routes>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
