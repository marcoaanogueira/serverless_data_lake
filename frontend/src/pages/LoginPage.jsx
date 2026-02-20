import React, { useState } from 'react';
import { Mail, Lock, LogIn, AlertCircle } from 'lucide-react';
import {
  SketchyCard,
  SketchyButton,
  SketchyInput,
  SketchyLabel,
  FloatingDecorations,
} from '@/components/ui/sketchy';

const API_BASE_URL = import.meta.env.VITE_API_URL || '';

export default function LoginPage({ onLogin }) {
  const [email, setEmail] = useState('');
  const [password, setPassword] = useState('');
  const [error, setError] = useState('');
  const [loading, setLoading] = useState(false);

  const handleSubmit = async (e) => {
    e.preventDefault();
    setError('');
    setLoading(true);

    try {
      const response = await fetch(`${API_BASE_URL}/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ email, password }),
      });

      const data = await response.json().catch(() => ({}));

      if (!response.ok) {
        setError(data.detail || 'Invalid email or password');
        return;
      }

      localStorage.setItem('dataLakeApiKey', data.token);
      onLogin();
    } catch {
      setError('Connection error. Check if the API is reachable.');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-gray-50 flex items-center justify-center">
      <FloatingDecorations />

      <div className="relative z-10 w-full max-w-sm px-4">
        {/* Logo */}
        <div className="text-center mb-8">
          <h1 className="text-4xl font-black text-[#1F2937]">
            Tadpole<span className="text-[#FBBF24]">.</span>
          </h1>
          <p className="text-gray-500 mt-2 font-medium">Data Platform</p>
        </div>

        <SketchyCard hover={false}>
          <form onSubmit={handleSubmit} className="space-y-5">
            <div className="space-y-1.5">
              <SketchyLabel htmlFor="email">Email</SketchyLabel>
              <div className="relative">
                <Mail className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
                <SketchyInput
                  id="email"
                  type="email"
                  placeholder="you@example.com"
                  value={email}
                  onChange={(e) => setEmail(e.target.value)}
                  className="pl-9"
                  required
                  autoFocus
                />
              </div>
            </div>

            <div className="space-y-1.5">
              <SketchyLabel htmlFor="password">Password</SketchyLabel>
              <div className="relative">
                <Lock className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-gray-400 pointer-events-none" />
                <SketchyInput
                  id="password"
                  type="password"
                  placeholder="••••••••"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  className="pl-9"
                  required
                />
              </div>
            </div>

            {error && (
              <div className="flex items-center gap-2 text-red-700 text-sm bg-red-50 rounded-xl p-3 border border-red-200">
                <AlertCircle className="w-4 h-4 shrink-0" />
                <span>{error}</span>
              </div>
            )}

            <SketchyButton
              type="submit"
              disabled={loading}
              color="mint"
              className="w-full justify-center"
            >
              {loading ? (
                <span className="flex items-center gap-2">
                  <span className="w-4 h-4 border-2 border-current border-t-transparent rounded-full animate-spin" />
                  Signing in...
                </span>
              ) : (
                <span className="flex items-center gap-2">
                  <LogIn className="w-4 h-4" />
                  Sign in
                </span>
              )}
            </SketchyButton>
          </form>
        </SketchyCard>
      </div>
    </div>
  );
}
