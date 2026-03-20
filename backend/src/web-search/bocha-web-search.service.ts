import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export type BochaWebSearchFreshness =
  | 'oneDay'
  | 'oneWeek'
  | 'oneMonth'
  | 'noLimit';

export interface BochaWebSearchParams {
  query: string;
  count?: number;
}

export interface NormalizedWebSearchResult {
  title: string;
  url: string;
  snippet: string | undefined;
  source: string | undefined;
}

export interface NormalizedWebSearchResponse {
  summary?: string;
  results: NormalizedWebSearchResult[];
  raw: unknown;
}

@Injectable()
export class BochaWebSearchService {
  constructor(private readonly configService: ConfigService) {}

  async search(params: BochaWebSearchParams) {
    // ({ query, count }: { query: string; count?: number }) => {
    const { query, count } = params;
    const apiKey = this.configService.get<string>('BOCHA_API_KEY');
    if (!apiKey) {
      return 'Bocha Web Search 的 API Key 未配置（环境变量 BOCHA_API_KEY），请先在服务端配置后再重试。';
    }

    const url = 'https://api.bochaai.com/v1/web-search';
    const body = {
      query,
      freshness: 'noLimit',
      summary: true,
      count: count ?? 10,
    };

    const response = await fetch(url, {
      method: 'POST',
      headers: {
        Authorization: `Bearer ${apiKey}`,
        'Content-Type': 'application/json',
      },
      body: JSON.stringify(body),
    });

    if (!response.ok) {
      const errorText = await response.text();
      console.error('搜索 API 请求失败:', errorText);
      return `搜索 API 请求失败，状态码: ${response.status}, 错误信息: ${errorText}`;
    }

    let json: any;
    try {
      json = await response.json();
    } catch (e) {
      return `搜索 API 请求失败，原因是：搜索结果解析失败 ${(e as Error).message}`;
    }

    try {
      if (json.code !== 200 || !json.data) {
        return `搜索 API 请求失败，原因是: ${json.msg ?? '未知错误'}`;
      }

      const webpages = json.data.webPages?.value ?? [];
      if (!webpages.length) {
        return '未找到相关结果。';
      }

      const formatted = webpages
        .map(
          (page: any, idx: number) => `引用: ${idx + 1}
                  标题: ${page.name}
                  URL: ${page.url}
                  摘要: ${page.summary}
                  网站名称: ${page.siteName}
                  网站图标: ${page.siteIcon}
                  发布时间: ${page.dateLastCrawled}`,
        )
        .join('\n\n');

      return formatted;
    } catch (e) {
      return `搜索 API 请求失败，原因是：搜索结果解析失败 ${(e as Error).message}`;
    }
  }
}
