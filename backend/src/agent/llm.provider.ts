import { Provider } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { ChatOpenAI } from '@langchain/openai';

export const CHAT_MODEL_TOKEN = 'CHAT_MODEL';
export const PLANNER_MODEL_TOKEN = 'PLANNER_MODEL';

function createChatOpenAI(configService: ConfigService, streaming: boolean) {
  const baseURL = configService.get<string>('OPENAI_BASE_URL');
  const apiKey = configService.get<string>('OPENAI_API_KEY');
  const model = configService.get<string>('LLM_MODEL_NAME', 'gpt-4o-mini');
  return new ChatOpenAI({
    apiKey,
    model,
    streaming,
    ...(baseURL ? { configuration: { baseURL } } : {}),
  });
}

export const chatModelProvider: Provider = {
  provide: CHAT_MODEL_TOKEN,
  inject: [ConfigService],
  useFactory: (configService: ConfigService) => createChatOpenAI(configService, true),
};

export const plannerModelProvider: Provider = {
  provide: PLANNER_MODEL_TOKEN,
  inject: [ConfigService],
  useFactory: (configService: ConfigService) => createChatOpenAI(configService, false),
};
