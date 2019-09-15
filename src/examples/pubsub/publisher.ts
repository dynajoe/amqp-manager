import { AmqpManager, publish } from '../../'
import { AmqpConfig } from './config'
import * as Minimist from 'minimist'
import * as _ from 'lodash'
import * as Debug from 'debug'

const Log = Debug('example')

async function Main(argv: Minimist.ParsedArgs) {
   const amqp_manager = new AmqpManager(AmqpConfig)

   const config = {
      rate: _.defaultTo(argv['rate'], 1),
      confirm: Boolean(_.defaultTo(argv['confirm'], 'true')),
   }

   Log('Publisher Configuration: [%o]', config)

   setInterval(async () => {
      try {
         const data = { key: Date.now().toString() }

         await publish(amqp_manager, {
            confirm: config.confirm,
            data: Buffer.from(JSON.stringify(data)),
            exchange: 'example.ex',
         })

         Log('Published [%o]', data)
      } catch (error) {
         Log('Error', error)
      }
   }, Math.floor(1000 / config.rate))
}

Main(Minimist(process.argv.slice(2)))
