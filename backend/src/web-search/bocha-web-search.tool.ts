import { DynamicTool } from '@langchain/core/tools';
import { BochaWebSearchService } from './bocha-web-search.service';

export function createBochaWebSearchTool(service: BochaWebSearchService) {
  return new DynamicTool({
    name: 'web_search',
    description:
      'Search the internet via Bocha web-search. Input should be a concise search query string.',
    func: async (query) => {
      const res = await service.search({ query });
      return res;
    },
  });
}
