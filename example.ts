'use strict'

import { subscribe, publish, AckInput } from './src/pubsub'

const AmqpManager = require('./dist/index').AmqpManager

const Config = {
   AMQP_USER: 'guest',
   AMQP_PASSWORD: 'guest',
   AMQP_HOST: '127.0.0.1',
   AMQP_PORT: '5672',
   AMQP_VHOST: '/',
}

const AmqpConfig = {
   amqplib: {
      connection: {
         username: Config.AMQP_USER,
         password: Config.AMQP_PASSWORD,
         hostname: Config.AMQP_HOST,
         port: Config.AMQP_PORT,
         vhost: Config.AMQP_VHOST,
      },
   },
   exchanges: [
      {
         exchange: 'example.ex',
         type: 'fanout',
         options: { durable: true },
      },
      {
         exchange: 'example.dead.ex',
         type: 'fanout',
         options: { durable: true },
      },
   ],
   queues: [
      {
         queue: 'test.q',
         options: {
            durable: true,
            autoDelete: false,
            exclusive: false,
            deadLetterExchange: 'example.dead.ex',
         },
      },
      {
         queue: 'example.dead.q',
         options: {
            durable: true,
            autoDelete: false,
            exclusive: false,
         },
      },
   ],
   bindings: [
      {
         exchange: 'example.ex',
         queue: 'test.q',
      },
      {
         exchange: 'example.dead.ex',
         queue: 'example.dead.q',
      },
   ],
}

const RunApp = async () => {
   const amqp_manager = new AmqpManager(AmqpConfig)
   const messages_in_flight = {}

   subscribe(amqp_manager, {
      queue: 'test.q',
      channel_name: 'inbound',
      prefetch_count: 10,
      retry_queue: null,
      dead_letter_exchange: null,
      dead_letter_queue: null,
      max_retry_count: 0,
      onError: error => {
         console.log(error)
      },
      handler: async (input: AckInput<{ key: string }>) => {
         await input.ack()

         if (messages_in_flight[input.message.key] == null) {
            console.log('Key not found', input.message.key)
         }

         delete messages_in_flight[input.message.key]
      },
      parser: message => JSON.parse(message.content.toString()),
   })

   setInterval(async () => {
      try {
         const data = { key: Date.now().toString() }

         messages_in_flight[data.key] = data

         await publish(amqp_manager, {
            confirm: true,
            data: Buffer.from(JSON.stringify(data)),
            exchange: 'example.ex',
            amqp_options: null,
         })
      } catch (error) {
         console.log(error)
      }
   }, 10)

   setInterval(() => {
      console.log('Messages in flight', Object.keys(messages_in_flight).length)
   }, 1000)
}

RunApp()
