import { AmqpManager, subscribe, AckInput } from '../../'
import { AmqpConfig } from './config'

async function Main() {
   const amqp_manager = new AmqpManager(AmqpConfig)

   subscribe(amqp_manager, {
      queue: 'test.q',
      channel_name: 'inbound',
      prefetch_count: 10,
      retry_queue: 'example.retry.q',
      dead_letter_exchange: 'example.dead.ex',
      dead_letter_queue: 'example.dead.q',
      max_retry_count: 3,
      onError: error => {
         console.log(error)
      },
      handler: async (input: AckInput<{ key: string }>) => {
         console.log('Received ', input.message)
         await input.ack()
      },
      parser: message => JSON.parse(message.content.toString()),
   })
}

Main()
