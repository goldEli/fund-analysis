import { Module } from '@nestjs/common';
import { BochaWebSearchService } from './bocha-web-search.service';

@Module({
  providers: [BochaWebSearchService],
  exports: [BochaWebSearchService],
})
export class WebSearchModule {}

