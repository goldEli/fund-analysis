import { Body, Controller, Post, Res } from '@nestjs/common';
import { Response } from 'express';
import { AgentService } from './agent.service';

@Controller('agent')
export class AgentController {
  constructor(private readonly agentService: AgentService) {}

  @Post('chat')
  async chat(@Body() body: { messages: { role: string; content: string }[] }, @Res() res: Response) {
    const stream = await this.agentService.chatStream(body.messages);

    res.setHeader('Content-Type', 'text/plain; charset=utf-8');
    res.setHeader('Transfer-Encoding', 'chunked');

    for await (const chunk of stream) {
      res.write(`0:${JSON.stringify(chunk)}\n`);
    }

    res.end();
  }
}
