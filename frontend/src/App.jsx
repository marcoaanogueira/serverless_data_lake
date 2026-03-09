import React from 'react';
import { BrowserRouter as Router, Routes, Route, Navigate, useNavigate } from 'react-router-dom';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import DataPlatform from './pages/DataPlatform';
import LoginPage from './pages/LoginPage';
import LandingPage from './pages/LandingPage';
import DocsPage from './pages/DocsPage';

// Landing page is opt-in: set VITE_LANDING_PAGE=1 in .env.local to enable it.
// By default the app starts at /login (suitable for self-hosted deploys).
const LANDING_ENABLED = import.meta.env.VITE_LANDING_PAGE === '1';

const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      staleTime: 1000 * 60 * 5,
      retry: 1,
    },
  },
});

function ProtectedRoute({ children }) {
  return !!sessionStorage.getItem('dataLakeApiKey')
    ? children
    : <Navigate to="/login" replace />;
}

function AppRoutes() {
  const navigate = useNavigate();

  const handleLogout = () => {
    sessionStorage.removeItem('dataLakeApiKey');
    navigate('/');
  };

  return (
    <Routes>
      {LANDING_ENABLED ? (
        <>
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
        </>
      ) : (
        <Route path="/" element={<Navigate to="/login" replace />} />
      )}
      <Route path="/login" element={<LoginPage onLogin={() => navigate('/platform')} />} />
      <Route
        path="/platform/*"
        element={
          <ProtectedRoute>
            <DataPlatform onLogout={handleLogout} />
          </ProtectedRoute>
        }
      />
      <Route path="*" element={<Navigate to="/login" replace />} />
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
