import { Test, TestingModule } from '@nestjs/testing';
import { ConfigService } from '@nestjs/config';
import { BochaWebSearchService } from './bocha-web-search.service';

describe('BochaWebSearchService', () => {
  let service: BochaWebSearchService;
  let configService: { get: jest.Mock };

  beforeEach(async () => {
    configService = { get: jest.fn() };

    const module: TestingModule = await Test.createTestingModule({
      providers: [
        BochaWebSearchService,
        {
          provide: ConfigService,
          useValue: configService,
        },
      ],
    }).compile();

    service = module.get<BochaWebSearchService>(BochaWebSearchService);
    (global as any).fetch = jest.fn();
  });

  afterEach(() => {
    jest.restoreAllMocks();
  });

  it('throws when BOCHA key is missing', async () => {
    configService.get.mockReturnValue(undefined);
    await expect(service.search({ query: 'test' })).rejects.toThrow(
      'BOCHA_API_KEY (or BOCHA_API_KEY) is not set',
    );
  });

  it('calls Bocha web-search with defaults and normalizes response', async () => {
    configService.get.mockImplementation((key: string) => {
      if (key === 'BOCHA_API_KEY') return 'k1';
      return undefined;
    });

    const fetchMock = (global as any).fetch as jest.Mock;
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({
        data: {
          summary: 'sum',
          results: [
            {
              title: 't',
              url: 'https://example.com',
              snippet: 'sn',
              source: 'ex',
            },
          ],
        },
      }),
    });

    const res = await service.search({ query: 'hello' });

    expect(fetchMock).toHaveBeenCalledTimes(1);
    const [url, init] = fetchMock.mock.calls[0];
    expect(url).toBe('https://api.bocha.cn/v1/web-search');
    expect(init.method).toBe('POST');
    expect(init.headers.Authorization).toBe('Bearer k1');
    expect(init.headers['Content-Type']).toBe('application/json');

    const body = JSON.parse(init.body);
    expect(body).toEqual({
      query: 'hello',
      summary: true,
      freshness: 'noLimit',
      count: 5,
    });

    expect(res.summary).toBe('sum');
    expect(res.results).toEqual([
      {
        title: 't',
        url: 'https://example.com',
        snippet: 'sn',
        source: 'ex',
      },
    ]);
    expect(res.raw).toBeDefined();
  });

  it('clamps count to [1, 10]', async () => {
    configService.get.mockImplementation((key: string) => {
      if (key === 'BOCHA_API_KEY') return 'k1';
      return undefined;
    });

    const fetchMock = (global as any).fetch as jest.Mock;
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ data: [] }),
    });

    await service.search({ query: 'x', count: 999 });
    const [, initHigh] = fetchMock.mock.calls[0];
    expect(JSON.parse(initHigh.body).count).toBe(10);

    fetchMock.mockClear();

    await service.search({ query: 'x', count: 0 });
    const [, initLow] = fetchMock.mock.calls[0];
    expect(JSON.parse(initLow.body).count).toBe(1);
  });

  it('uses BOCHA_API_KEY fallback', async () => {
    configService.get.mockImplementation((key: string) => {
      if (key === 'BOCHA_API_KEY') return undefined;
      if (key === 'BOCHA_API_KEY') return 'k2';
      return undefined;
    });

    const fetchMock = (global as any).fetch as jest.Mock;
    fetchMock.mockResolvedValue({
      ok: true,
      status: 200,
      statusText: 'OK',
      json: async () => ({ data: [] }),
    });

    await service.search({ query: 'hello' });
    const [, init] = fetchMock.mock.calls[0];
    expect(init.headers.Authorization).toBe('Bearer k2');
  });

  it('throws with status and body when Bocha returns non-2xx', async () => {
    configService.get.mockImplementation((key: string) => {
      if (key === 'BOCHA_API_KEY') return 'k1';
      return undefined;
    });

    const fetchMock = (global as any).fetch as jest.Mock;
    fetchMock.mockResolvedValue({
      ok: false,
      status: 401,
      statusText: 'Unauthorized',
      text: async () => 'bad key',
    });

    await expect(service.search({ query: 'hello' })).rejects.toThrow(
      'Bocha web-search failed: 401 Unauthorized - bad key',
    );
  });
});

