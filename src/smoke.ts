import { NestFactory } from '@nestjs/core'
import { AppModule } from './app.module.js'
import { MESSAGE_BUS, MessageBus } from './messaging/message-bus.port.js'

async function main() {
  const app = await NestFactory.createApplicationContext(AppModule)
  const bus = app.get<MessageBus>(MESSAGE_BUS)

  await bus.subscribe(async (msg) => {
    console.log('received', msg.deliveryId, 'attempt', msg.attempt)
    throw new Error('forced failure')
  })

  await bus.publish({ smoke: true })
}
main()
