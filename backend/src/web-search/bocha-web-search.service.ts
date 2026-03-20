import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';

export type BochaWebSearchFreshness = 'oneDay' | 'oneWeek' | 'oneMonth' | 'noLimit';

export interface BochaWebSearchParams {
  query: string;
  summary?: boolean;
  freshness?: BochaWebSearchFreshness;
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

  async search(params: BochaWebSearchParams): Promise<NormalizedWebSearchResponse> {
    const apiKey =
      this.configService.get<string>('BOCHA-API-KEY') ??
      this.configService.get<string>('BOCHA_API_KEY');
    if (!apiKey) {
      throw new Error('BOCHA-API-KEY (or BOCHA_API_KEY) is not set');
    }

    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 20_000);

    try {
      const res = await fetch('https://api.bocha.cn/v1/web-search', {
        method: 'POST',
        headers: {
          Authorization: `Bearer ${apiKey}`,
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query: params.query,
          summary: params.summary ?? true,
          freshness: params.freshness ?? 'noLimit',
          count: Math.max(1, Math.min(10, params.count ?? 5)),
        }),
        signal: controller.signal,
      });

      if (!res.ok) {
        const bodyText = await res.text().catch(() => '');
        throw new Error(`Bocha web-search failed: ${res.status} ${res.statusText}${bodyText ? ` - ${bodyText}` : ''}`);
      }

      const json = (await res.json()) as unknown;
      const normalized = this.normalize(json);
      return { ...normalized, raw: json };
    } finally {
      clearTimeout(timeout);
    }
  }

  private normalize(raw: unknown): Omit<NormalizedWebSearchResponse, 'raw'> {
    const obj = raw as any;

    const summary =
      (typeof obj?.summary === 'string' ? obj.summary : undefined) ??
      (typeof obj?.data?.summary === 'string' ? obj.data.summary : undefined) ??
      (typeof obj?.result?.summary === 'string' ? obj.result.summary : undefined);

    const candidates: any[] =
      (Array.isArray(obj?.data) ? obj.data : undefined) ??
      (Array.isArray(obj?.data?.items) ? obj.data.items : undefined) ??
      (Array.isArray(obj?.data?.results) ? obj.data.results : undefined) ??
      (Array.isArray(obj?.results) ? obj.results : undefined) ??
      (Array.isArray(obj?.items) ? obj.items : undefined) ??
      (Array.isArray(obj?.webPages?.value) ? obj.webPages.value : undefined) ??
      [];

    const results = candidates
      .map((item: any) => {
        const title = item?.title ?? item?.name ?? item?.headline;
        const url = item?.url ?? item?.link ?? item?.site ?? item?.sourceUrl;
        const snippet = item?.snippet ?? item?.summary ?? item?.description ?? item?.content;
        const source = item?.source ?? item?.siteName ?? item?.provider;

        if (typeof title !== 'string' || typeof url !== 'string') return null;

        return {
          title,
          url,
          snippet: typeof snippet === 'string' ? snippet : undefined,
          source: typeof source === 'string' ? source : undefined,
        };
      })
      .filter((x): x is NormalizedWebSearchResult => x !== null);

    return { summary, results };
  }
}
