import { Module } from '@nestjs/common'
import { ConfigModule } from '@nestjs/config'
import { MessagingModule } from './messaging/messaging.module.js'

@Module({
  imports: [ConfigModule.forRoot({ isGlobal: true }), MessagingModule],
})
export class AppModule {}
