import { Injectable, Inject } from '@nestjs/common';
import {
  BaseMessage,
  SystemMessage,
  ToolMessage,
} from '@langchain/core/messages';
import { ChatOpenAI } from '@langchain/openai';
import { DynamicTool } from '@langchain/core/tools';
import { PLANNER_MODEL_TOKEN } from './llm.provider';

@Injectable()
export class ToolOrchestratorService {
  constructor(
    @Inject(PLANNER_MODEL_TOKEN) private plannerModel: ChatOpenAI,
    @Inject('bocha_web_search') private webSearchTool: any,
  ) {}

  async runToolLoop(langchainMessages: BaseMessage[]): Promise<BaseMessage[]> {
    const tools = [this.webSearchTool];

    const toolPrompt = new SystemMessage(
      'You may use the web_search tool to search the internet for sources and URLs when needed. Prefer using it when answering questions that require external facts or citations.',
    );

    const workingMessages: BaseMessage[] = [toolPrompt, ...langchainMessages];

    const plannerWithTools = (this.plannerModel as any).bindTools
      ? (this.plannerModel as any).bindTools(tools)
      : this.plannerModel;

    while (true) {
      const ai = await plannerWithTools.invoke(workingMessages);
      workingMessages.push(ai);

      const toolCalls: any[] =
        (ai as any).tool_calls ??
        (ai as any).additional_kwargs?.tool_calls ??
        [];

      console.log('toolCalls', toolCalls);

      if (!Array.isArray(toolCalls) || toolCalls.length === 0) break;

      for (const call of toolCalls) {
        const name = call?.name ?? call?.function?.name;
        // const rawArgs = call?.args ?? call?.function?.arguments ?? {};
        const id = call?.id ?? call?.tool_call_id;

        // const tool = tools.find((t) => t.name === name);
        // if (!tool || typeof id !== 'string') continue;


        // console.log("toolInput", toolInput)
        if (name == 'web_search') {
          console.log('web_search',  call.args as any);
          const result = await this.webSearchTool.invoke(call.args as any);
          console.log('web_search result', result);
          workingMessages.push(
            new ToolMessage({
              tool_call_id: id,
              content:
                typeof result === 'string' ? result : JSON.stringify(result),
            }),
          );
        } else {
          workingMessages.push(
            new ToolMessage({
              tool_call_id: id,
              content: '工具不存在',
            }),
          );
        }
      }
    }

    return workingMessages;
  }
}
