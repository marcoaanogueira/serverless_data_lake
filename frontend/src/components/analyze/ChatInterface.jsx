import React, { useState, useRef, useEffect } from 'react';
import { useQuery, useMutation, useQueryClient } from '@tanstack/react-query';
import { MessageSquare, Plus, Trash2, Loader2, BarChart3 } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import dataLakeApi from '@/api/dataLakeClient';
import ChatMessage from './ChatMessage';
import ChatInput from './ChatInput';
import { SketchyCard, SketchyBadge } from '@/components/ui/sketchy';

export default function ChatInterface() {
  const [activeSessionId, setActiveSessionId] = useState(null);
  const [messages, setMessages] = useState([]);
  const [isLoading, setIsLoading] = useState(false);
  const messagesEndRef = useRef(null);
  const queryClient = useQueryClient();

  // Fetch sessions list
  const { data: sessions = [] } = useQuery({
    queryKey: ['chatSessions'],
    queryFn: () => dataLakeApi.chat.getSessions(),
  });

  // Load session messages when active session changes
  useEffect(() => {
    if (!activeSessionId) {
      setMessages([]);
      return;
    }
    dataLakeApi.chat.getSession(activeSessionId)
      .then(data => setMessages(data.messages || []))
      .catch(() => setMessages([]));
  }, [activeSessionId]);

  // Auto-scroll to bottom
  useEffect(() => {
    messagesEndRef.current?.scrollIntoView({ behavior: 'smooth' });
  }, [messages]);

  const handleSend = async (text) => {
    // Optimistically add user message
    const userMsg = {
      message_id: `temp-${Date.now()}`,
      role: 'user',
      content: [{ type: 'text', text }],
      created_at: new Date().toISOString(),
    };
    setMessages(prev => [...prev, userMsg]);
    setIsLoading(true);

    try {
      const response = await dataLakeApi.chat.sendMessage(activeSessionId, text);

      // If a new session was created, set it as active
      if (!activeSessionId && response.session_id) {
        setActiveSessionId(response.session_id);
        queryClient.invalidateQueries({ queryKey: ['chatSessions'] });
      }

      // Add assistant response
      const assistantMsg = {
        message_id: response.message_id,
        role: 'assistant',
        content: response.content,
        created_at: new Date().toISOString(),
      };
      setMessages(prev => [...prev, assistantMsg]);
    } catch (error) {
      // Show error as assistant message
      setMessages(prev => [...prev, {
        message_id: `error-${Date.now()}`,
        role: 'assistant',
        content: [{ type: 'text', text: `Error: ${error.message || 'Failed to get response'}` }],
        created_at: new Date().toISOString(),
      }]);
    } finally {
      setIsLoading(false);
      queryClient.invalidateQueries({ queryKey: ['chatSessions'] });
    }
  };

  const handleNewChat = () => {
    setActiveSessionId(null);
    setMessages([]);
  };

  const handleDeleteSession = async (e, sessionId) => {
    e.stopPropagation();
    try {
      await dataLakeApi.chat.deleteSession(sessionId);
      queryClient.invalidateQueries({ queryKey: ['chatSessions'] });
      if (activeSessionId === sessionId) {
        handleNewChat();
      }
    } catch {
      // Ignore delete errors
    }
  };

  const isEmpty = messages.length === 0;

  return (
    <div className="grid grid-cols-12 gap-6" style={{ minHeight: 'calc(100vh - 200px)' }}>
      {/* Sidebar - Sessions */}
      <div className="col-span-12 lg:col-span-3">
        <SketchyCard className="sticky top-24 p-4">
          <div className="flex items-center justify-between mb-4">
            <h3 className="font-black text-gray-900">Chats</h3>
            <button
              onClick={handleNewChat}
              className="w-8 h-8 flex items-center justify-center rounded-xl bg-[#1F2937] text-white hover:bg-[#374151] transition-colors"
              title="New chat"
            >
              <Plus className="w-4 h-4" />
            </button>
          </div>

          <div className="space-y-1 max-h-[60vh] overflow-y-auto">
            {sessions.length === 0 ? (
              <p className="text-xs text-gray-400 text-center py-4">No conversations yet</p>
            ) : (
              sessions.map(session => (
                <button
                  key={session.session_id}
                  onClick={() => setActiveSessionId(session.session_id)}
                  className={`w-full flex items-center gap-2 px-3 py-2 rounded-xl text-left text-sm transition-colors group ${
                    activeSessionId === session.session_id
                      ? 'bg-[#1F2937] text-white'
                      : 'text-gray-600 hover:bg-gray-50'
                  }`}
                >
                  <MessageSquare className="w-3.5 h-3.5 flex-shrink-0" />
                  <span className="truncate flex-1">{session.title || 'New Chat'}</span>
                  <button
                    onClick={(e) => handleDeleteSession(e, session.session_id)}
                    className={`opacity-0 group-hover:opacity-100 p-1 rounded hover:bg-red-100 transition-all ${
                      activeSessionId === session.session_id ? 'hover:bg-red-900/20' : ''
                    }`}
                  >
                    <Trash2 className="w-3 h-3" />
                  </button>
                </button>
              ))
            )}
          </div>
        </SketchyCard>
      </div>

      {/* Main Chat Area */}
      <div className="col-span-12 lg:col-span-9 flex flex-col">
        <SketchyCard className="flex-1 flex flex-col p-0 overflow-hidden">
          {/* Messages */}
          <div className="flex-1 overflow-y-auto p-6 space-y-4" style={{ minHeight: '400px', maxHeight: 'calc(100vh - 340px)' }}>
            {isEmpty ? (
              <div className="flex flex-col items-center justify-center h-full text-center">
                <div className="w-16 h-16 rounded-2xl bg-[#1F2937] flex items-center justify-center mb-4" style={{ boxShadow: '4px 4px 0 rgba(0,0,0,0.15)' }}>
                  <BarChart3 className="w-8 h-8 text-white" />
                </div>
                <h3 className="text-xl font-black text-gray-900 mb-2">Analytics Chat</h3>
                <p className="text-sm text-gray-500 max-w-md mb-6">
                  Ask questions about your data in natural language.
                  I'll write SQL queries, analyze results, and create charts for you.
                </p>
              </div>
            ) : (
              <AnimatePresence initial={false}>
                {messages.map((msg) => (
                  <motion.div
                    key={msg.message_id}
                    initial={{ opacity: 0, y: 10 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ duration: 0.2 }}
                  >
                    <ChatMessage message={msg} />
                  </motion.div>
                ))}
              </AnimatePresence>
            )}

            {isLoading && (
              <div className="flex items-center gap-3">
                <div className="w-8 h-8 rounded-xl bg-[#1F2937] flex items-center justify-center">
                  <Loader2 className="w-4 h-4 text-white animate-spin" />
                </div>
                <div className="bg-white border-2 border-gray-100 rounded-2xl px-4 py-3" style={{ boxShadow: '3px 3px 0 rgba(0,0,0,0.06)' }}>
                  <div className="flex gap-1">
                    <div className="w-2 h-2 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: '0ms' }} />
                    <div className="w-2 h-2 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: '150ms' }} />
                    <div className="w-2 h-2 bg-gray-300 rounded-full animate-bounce" style={{ animationDelay: '300ms' }} />
                  </div>
                </div>
              </div>
            )}

            <div ref={messagesEndRef} />
          </div>

          {/* Input */}
          <div className="border-t-2 border-gray-100 p-4">
            <ChatInput
              onSend={handleSend}
              isLoading={isLoading}
              showSuggestions={isEmpty}
            />
          </div>
        </SketchyCard>
      </div>
    </div>
  );
}
