import { Injectable, Inject } from '@nestjs/common';
import { BaseMessage, SystemMessage, ToolMessage } from '@langchain/core/messages';
import { ChatOpenAI } from '@langchain/openai';
import { DynamicTool } from '@langchain/core/tools';
import { PLANNER_MODEL_TOKEN } from './llm.provider';
import { WEB_SEARCH_TOOL_TOKEN } from './web-search-tool.provider';

@Injectable()
export class ToolOrchestratorService {
  constructor(
    @Inject(PLANNER_MODEL_TOKEN) private plannerModel: ChatOpenAI,
    @Inject(WEB_SEARCH_TOOL_TOKEN) private webSearchTool: DynamicTool,
  ) {}

  async runToolLoop(langchainMessages: BaseMessage[]): Promise<BaseMessage[]> {
    const tools = [this.webSearchTool];

    const toolPrompt = new SystemMessage(
      'You may use the bocha_web_search tool to search the internet for sources and URLs when needed. Prefer using it when answering questions that require external facts or citations.',
    );

    const workingMessages: BaseMessage[] = [toolPrompt, ...langchainMessages];

    const plannerWithTools = (this.plannerModel as any).bindTools
      ? (this.plannerModel as any).bindTools(tools)
      : this.plannerModel;

    for (let i = 0; i < 3; i++) {
      const ai = await plannerWithTools.invoke(workingMessages);
      workingMessages.push(ai);

      const toolCalls: any[] =
        (ai as any).tool_calls ?? (ai as any).additional_kwargs?.tool_calls ?? [];

      if (!Array.isArray(toolCalls) || toolCalls.length === 0) break;

      for (const call of toolCalls) {
        const name = call?.name ?? call?.function?.name;
        console.log("tool name", name)
        const rawArgs = call?.args ?? call?.function?.arguments ?? {};
        const id = call?.id ?? call?.tool_call_id;

        const tool = tools.find((t) => t.name === name);
        if (!tool || typeof id !== 'string') continue;

        let parsedArgs: unknown = rawArgs;
        if (typeof rawArgs === 'string') {
          try {
            parsedArgs = JSON.parse(rawArgs);
          } catch {
            parsedArgs = { query: rawArgs };
          }
        }

        let toolInput: unknown = parsedArgs;
        if (parsedArgs && typeof parsedArgs === 'object' && 'query' in (parsedArgs as any)) {
          toolInput = (parsedArgs as any).query;
        }
        if (typeof toolInput !== 'string') {
          toolInput = JSON.stringify(toolInput);
        }

        const result = await tool.invoke(toolInput as any);
        workingMessages.push(
          new ToolMessage({
            tool_call_id: id,
            content: typeof result === 'string' ? result : JSON.stringify(result),
          }),
        );
      }
    }

    return workingMessages;
  }
}
