import { Module } from '@nestjs/common'
import { ConfigService } from '@nestjs/config'
import { MESSAGE_BUS } from './message-bus.port.js'
import { InMemoryBus } from './in-memory.bus.js'
import { RabbitMqBus } from './rabbitmq.bus.js'
import { retryDelaysFromConfig } from './retry-delays.js'

/**
 * The adapter is chosen at boot by MESSAGE_BUS_DRIVER (rabbitmq | memory).
 *
 * This is the safety net for the time-box: if the broker is unreachable, one env var
 * keeps the system running with identical semantics. It is also what lets the test
 * suite run without Docker.
 */
@Module({
  providers: [
    {
      provide: MESSAGE_BUS,
      inject: [ConfigService],
      useFactory: (config: ConfigService) => {
        const driver = config.get<string>('MESSAGE_BUS_DRIVER', 'rabbitmq')

        if (driver === 'memory') return new InMemoryBus({ retryDelaysMs: retryDelaysFromConfig(config) })

        return new RabbitMqBus(config)
      },
    },
  ],
  exports: [MESSAGE_BUS],
})
export class MessagingModule {}
