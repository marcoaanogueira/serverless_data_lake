import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import DataPlatform from './pages/DataPlatform';
import LoginPage from './pages/LoginPage';
import LandingPage from './pages/LandingPage';
import DocsPage from './pages/DocsPage';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5,
      retry: 1,
    },
  },
});

function ProtectedRoute({ children }) {
  return !!localStorage.getItem('dataLakeApiKey')
    ? children
    : <Navigate to="/login" replace />;
}

function AppRoutes() {
  const navigate = useNavigate();

  const handleLogout = () => {
    localStorage.removeItem('dataLakeApiKey');
    navigate('/');
  };

  return (
    <Routes>
      <Route
        path="/"
        element={
          <LandingPage
            onGetStarted={() => navigate('/login')}
            onDocs={() => navigate('/docs')}
          />
        }
      />
      <Route path="/docs" element={<DocsPage onBack={() => navigate('/')} />} />
      <Route path="/login" element={<LoginPage onLogin={() => navigate('/platform')} />} />
      <Route
        path="/platform/*"
        element={
          <ProtectedRoute>
            <DataPlatform onLogout={handleLogout} />
          </ProtectedRoute>
        }
      />
      <Route path="*" element={<Navigate to="/" replace />} />
    </Routes>
  );
}

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <Router>
        <AppRoutes />
      </Router>
    </QueryClientProvider>
  );
}
