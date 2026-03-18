import { Injectable } from '@nestjs/common';
import { InjectRepository } from '@nestjs/typeorm';
import { Repository } from 'typeorm';
import { Message } from './message.entity';
import { ChatOpenAI } from '@langchain/openai';
import { HumanMessage, AIMessage, SystemMessage } from '@langchain/core/messages';
import { ConfigService } from '@nestjs/config';

@Injectable()
export class AgentService {
  private chatModel: ChatOpenAI;

  constructor(
    @InjectRepository(Message)
    private messageRepository: Repository<Message>,
    private configService: ConfigService,
  ) {
    const baseURL = this.configService.get<string>('OPENAI_BASE_URL');
    this.chatModel = new ChatOpenAI({
      apiKey: this.configService.get<string>('OPENAI_API_KEY'),
      model: this.configService.get<string>('LLM_MODEL_NAME', 'gpt-4o-mini'),
      streaming: true,
      ...(baseURL ? { configuration: { baseURL } } : {}),
    });
  }

  async getChatHistory(): Promise<Message[]> {
    return this.messageRepository.find({
      order: { createdAt: 'ASC' },
    });
  }

  async chatStream(messages: { role: string; content: string }[]) {
    // Save user message (assuming the last one is the new user message)
    const lastUserMessage = messages[messages.length - 1];
    if (lastUserMessage && lastUserMessage.role === 'user') {
      const userMsg = this.messageRepository.create({
        role: 'user',
        content: lastUserMessage.content,
      });
      await this.messageRepository.save(userMsg);
    }

    const langchainMessages = messages.map((m) => {
      if (m.role === 'user') return new HumanMessage(m.content);
      if (m.role === 'assistant') return new AIMessage(m.content);
      return new SystemMessage(m.content);
    });

    const stream = await this.chatModel.stream(langchainMessages);

    // We will use an async generator to yield chunks and then save the final message
    async function* generateStream(this: AgentService) {
      let fullResponse = '';
      for await (const chunk of stream) {
        const text = chunk.content as string;
        fullResponse += text;
        yield text;
      }
      // Save assistant message after streaming completes
      const assistantMsg = this.messageRepository.create({
        role: 'assistant',
        content: fullResponse,
      });
      await this.messageRepository.save(assistantMsg);
    }

    return generateStream.bind(this)();
  }
}
