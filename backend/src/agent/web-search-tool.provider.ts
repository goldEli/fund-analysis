import { Provider } from '@nestjs/common';
import { DynamicTool } from '@langchain/core/tools';
import { BochaWebSearchService } from '../web-search/bocha-web-search.service';
import { createBochaWebSearchTool } from '../web-search/bocha-web-search.tool';

export const WEB_SEARCH_TOOL_TOKEN = 'WEB_SEARCH_TOOL';

export const webSearchToolProvider: Provider<DynamicTool> = {
  provide: WEB_SEARCH_TOOL_TOKEN,
  inject: [BochaWebSearchService],
  useFactory: (bochaWebSearchService: BochaWebSearchService) =>
    createBochaWebSearchTool(bochaWebSearchService),
};

