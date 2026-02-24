import React from 'react';
import { Bot, User } from 'lucide-react';
import ChartRenderer from './ChartRenderer';

export default function ChatMessage({ message }) {
  const isUser = message.role === 'user';
  const content = message.content || [];

  return (
    <div className={`flex gap-3 ${isUser ? 'justify-end' : 'justify-start'}`}>
      {!isUser && (
        <div className="w-8 h-8 rounded-xl bg-[#1F2937] flex items-center justify-center flex-shrink-0 mt-1">
          <Bot className="w-4 h-4 text-white" />
        </div>
      )}

      <div className={`max-w-[85%] space-y-2 ${isUser ? 'items-end' : 'items-start'}`}>
        {content.map((block, i) => {
          if (block.type === 'text') {
            return (
              <div
                key={i}
                className={`rounded-2xl px-4 py-3 text-sm leading-relaxed whitespace-pre-wrap ${
                  isUser
                    ? 'bg-[#1F2937] text-white'
                    : 'bg-white border-2 border-gray-100 text-gray-800'
                }`}
                style={isUser ? { boxShadow: '3px 3px 0 rgba(0,0,0,0.15)' } : { boxShadow: '3px 3px 0 rgba(0,0,0,0.06)' }}
              >
                {block.text}
              </div>
            );
          }
          if (block.type === 'chart') {
            return <ChartRenderer key={i} spec={block} />;
          }
          return null;
        })}
      </div>

      {isUser && (
        <div className="w-8 h-8 rounded-xl bg-[#A8E6CF] flex items-center justify-center flex-shrink-0 mt-1">
          <User className="w-4 h-4 text-[#065F46]" />
        </div>
      )}
    </div>
  );
}
