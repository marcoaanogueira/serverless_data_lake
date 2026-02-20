import React, { useState } from 'react';
import { BrowserRouter as Router, Routes, Route } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import DataPlatform from './pages/DataPlatform';
import LoginPage from './pages/LoginPage';

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
  const [isAuthenticated, setIsAuthenticated] = useState(
    () => !!localStorage.getItem('dataLakeApiKey')
  );

  const handleLogin = () => setIsAuthenticated(true);

  const handleLogout = () => {
    localStorage.removeItem('dataLakeApiKey');
    setIsAuthenticated(false);
  };

  if (!isAuthenticated) {
    return <LoginPage onLogin={handleLogin} />;
  }

  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <Routes>
          <Route path="/" element={<DataPlatform onLogout={handleLogout} />} />
          <Route path="/ai" element={<DataPlatform onLogout={handleLogout} />} />
          <Route path="/ingestion" element={<DataPlatform onLogout={handleLogout} />} />
          <Route path="/transform" element={<DataPlatform onLogout={handleLogout} />} />
          <Route path="/query" element={<DataPlatform onLogout={handleLogout} />} />
        </Routes>
      </Router>
    </QueryClientProvider>
  );
}

export default App;
