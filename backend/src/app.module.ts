import { Module } from '@nestjs/common';
import { ConfigModule, ConfigService } from '@nestjs/config';
import { TypeOrmModule } from '@nestjs/typeorm';
import { AppController } from './app.controller';
import { AppService } from './app.service';
import { AgentModule } from './agent/agent.module';
import { Message } from './agent/message.entity';
import * as path from 'path';

@Module({
  imports: [
    ConfigModule.forRoot({
      isGlobal: true,
      envFilePath: [
        path.join(__dirname, '..', '..', '.env'),
        path.join(__dirname, '..', '.env'),
      ],
    }),
    TypeOrmModule.forRootAsync({
      imports: [ConfigModule],
      useFactory: (configService: ConfigService) => {
        const databaseUrl = configService.get<string>('DATABASE_URL');
        if (databaseUrl) {
          return {
            type: 'postgres',
            url: databaseUrl,
            entities: [Message],
            synchronize: true,
          };
        }

        return {
          type: 'postgres',
          host: configService.get<string>('PGHOST', 'localhost'),
          port: Number(configService.get<string>('PGPORT', '5432')),
          username: configService.get<string>('PGUSER', 'postgres'),
          password: configService.get<string>('PGPASSWORD', ''),
          database: configService.get<string>('PGDATABASE', 'postgres'),
          entities: [Message],
          synchronize: true,
        };
      },
      inject: [ConfigService],
    }),
    AgentModule,
  ],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
