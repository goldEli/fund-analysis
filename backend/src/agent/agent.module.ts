import { Module } from '@nestjs/common';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AgentController } from './agent.controller';
import { AgentService } from './agent.service';
import { Message } from './message.entity';
import { WebSearchModule } from '../web-search/web-search.module';
import { chatModelProvider, plannerModelProvider } from './llm.provider';
import { ToolOrchestratorService } from './tool-orchestrator.service';
import { webSearchToolProvider } from './web-search-tool.provider';

@Module({
  imports: [TypeOrmModule.forFeature([Message]), WebSearchModule],
  controllers: [AgentController],
  providers: [
    chatModelProvider,
    plannerModelProvider,
    webSearchToolProvider,
    ToolOrchestratorService,
    AgentService
  ]
})
export class AgentModule {}
