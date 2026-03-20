import { Test, TestingModule } from '@nestjs/testing';
import { AgentService } from './agent.service';
import { getRepositoryToken } from '@nestjs/typeorm';
import { Message } from './message.entity';
import { CHAT_MODEL_TOKEN } from './llm.provider';
import { ToolOrchestratorService } from './tool-orchestrator.service';

describe('AgentService', () => {
  let service: AgentService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [
        AgentService,
        {
          provide: getRepositoryToken(Message),
          useValue: {
            create: jest.fn(),
            save: jest.fn(),
            find: jest.fn(),
          },
        },
        {
          provide: CHAT_MODEL_TOKEN,
          useValue: {
            stream: jest.fn(),
          },
        },
        {
          provide: ToolOrchestratorService,
          useValue: {
            runToolLoop: jest.fn(),
          },
        },
      ],
    }).compile();

    service = module.get<AgentService>(AgentService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
