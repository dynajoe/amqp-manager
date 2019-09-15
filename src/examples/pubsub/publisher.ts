import { AmqpManager, publish } from '../../'
import { AmqpConfig } from './config'

async function Main() {
   const amqp_manager = new AmqpManager(AmqpConfig)

   setInterval(async () => {
      try {
         const data = { key: Date.now().toString() }

         await publish(amqp_manager, {
            confirm: true,
            data: Buffer.from(JSON.stringify(data)),
            exchange: 'example.ex',
         })

         console.log('Published ', data)
      } catch (error) {
         console.log(error)
      }
   }, 1000)
}

Main()
