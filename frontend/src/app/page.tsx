'use client';

import { useChat } from 'ai/react';
import { Send, User, Bot } from 'lucide-react';

export default function ChatPage() {
  const { messages, input, handleInputChange, handleSubmit, isLoading } = useChat({
    api: 'http://localhost:3001/agent/chat',
  });

  return (
    <div className="flex flex-col h-screen bg-gray-50 dark:bg-zinc-950 text-gray-900 dark:text-gray-100 font-sans">
      <header className="p-4 border-b border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-900 shadow-sm z-10">
        <h1 className="text-xl font-semibold flex items-center gap-2">
          <Bot className="w-6 h-6 text-blue-500" />
          Streaming AI Agent
        </h1>
      </header>

      <main className="flex-1 overflow-y-auto p-4 md:p-6 space-y-6">
        {messages.length === 0 ? (
          <div className="flex flex-col items-center justify-center h-full text-gray-400 dark:text-gray-500">
            <Bot className="w-16 h-16 mb-4 opacity-50" />
            <p>Start a conversation with the AI agent!</p>
          </div>
        ) : (
          messages.map((m) => (
            <div
              key={m.id}
              className={`flex items-start gap-4 ${
                m.role === 'user' ? 'justify-end' : 'justify-start'
              }`}
            >
              {m.role !== 'user' && (
                <div className="w-8 h-8 rounded-full bg-blue-100 dark:bg-blue-900/30 flex items-center justify-center flex-shrink-0 mt-1">
                  <Bot className="w-5 h-5 text-blue-600 dark:text-blue-400" />
                </div>
              )}
              
              <div
                className={`max-w-[85%] md:max-w-[75%] rounded-2xl px-5 py-3.5 leading-relaxed ${
                  m.role === 'user'
                    ? 'bg-blue-600 text-white rounded-br-sm shadow-md'
                    : 'bg-white dark:bg-zinc-900 border border-gray-100 dark:border-zinc-800 shadow-sm rounded-bl-sm'
                }`}
              >
                <div className="whitespace-pre-wrap">{m.content}</div>
              </div>

              {m.role === 'user' && (
                <div className="w-8 h-8 rounded-full bg-gray-200 dark:bg-zinc-800 flex items-center justify-center flex-shrink-0 mt-1">
                  <User className="w-5 h-5 text-gray-500 dark:text-gray-400" />
                </div>
              )}
            </div>
          ))
        )}
      </main>

      <footer className="p-4 border-t border-gray-200 dark:border-zinc-800 bg-white dark:bg-zinc-900">
        <form
          onSubmit={handleSubmit}
          className="max-w-4xl mx-auto flex items-end gap-2 bg-gray-100 dark:bg-zinc-800 p-2 rounded-xl focus-within:ring-2 focus-within:ring-blue-500/50 transition-shadow"
        >
          <input
            className="flex-1 bg-transparent p-3 outline-none resize-none min-h-[44px] max-h-32"
            value={input}
            placeholder="Type your message..."
            onChange={handleInputChange}
            disabled={isLoading}
          />
          <button
            type="submit"
            disabled={isLoading || !input.trim()}
            className="p-3 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg transition-colors flex-shrink-0"
          >
            <Send className="w-5 h-5" />
          </button>
        </form>
      </footer>
    </div>
  );
}
