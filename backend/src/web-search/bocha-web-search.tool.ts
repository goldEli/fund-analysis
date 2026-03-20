import { DynamicTool } from '@langchain/core/tools';
import { BochaWebSearchService } from './bocha-web-search.service';

export function createBochaWebSearchTool(service: BochaWebSearchService) {
  return new DynamicTool({
    name: 'bocha_web_search',
    description:
      'Search the internet via Bocha web-search. Input should be a concise search query string.',
    func: async (query) => {
      console.log("bocha web search query", query)
      const res = await service.search({ query });
      const payload = {
        summary: res.summary,
        results: res.results.slice(0, 10),
      };
      console.log("bocha web search payload", payload)
      return JSON.stringify(payload);
    },
  });
}
