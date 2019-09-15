import { AmqpManager, subscribe, AckInput } from '../../'
import { AmqpConfig } from './config'
import * as Minimist from 'minimist'
import * as _ from 'lodash'
import * as Debug from 'debug'

const Log = Debug('example')

async function Main(argv: Minimist.ParsedArgs) {
   const amqp_manager = new AmqpManager(AmqpConfig)

   const config = {
      prefetch: _.defaultTo(argv['prefetch'], 1),
      retries: _.defaultTo(argv['retries'], 3),
      timeout: _.defaultTo(argv['timeout'], 60000),
      delay: _.defaultTo(argv['delay'], 100),
      error_rate: _.defaultTo(argv['error-rate'], 0),
   }

   Log('Subscriber Configuration: [%o]', config)

   subscribe(amqp_manager, {
      queue: 'test.q',
      channel_name: 'inbound',
      prefetch_count: config.prefetch,
      handler_timeout_ms: config.timeout,
      max_retry_count: config.retries,
      retry_queue: 'example.retry.q',
      dead_letter_exchange: 'example.dead.ex',
      dead_letter_queue: 'example.dead.q',
      onError: error => {
         Log('Error', error)
      },
      handler: async (input: AckInput<{ key: string }>) => {
         Log('Received [%o]', input.message)

         return new Promise(resolve => {
            setTimeout(async () => {
               if (config.error_rate > 0 && Math.random() < config.error_rate) {
                  await input.nack()
               } else {
                  await input.ack()
               }

               resolve()
            }, config.delay)
         })
      },
      parser: message => JSON.parse(message.content.toString()),
   })
}

Main(Minimist(process.argv.slice(2)))
