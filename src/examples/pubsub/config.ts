import * as T from '../../types'

export const AmqpConfig: T.AmqpConfig = {
   amqplib: {
      connection: {
         username: 'guest',
         password: 'guest',
         hostname: '127.0.0.1',
         port: 5672,
         vhost: '/',
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
            deadLetterExchange: '',
            deadLetterRoutingKey: 'example.retry.q',
         },
      },
      {
         queue: 'example.retry.q',
         options: {
            durable: true,
            autoDelete: false,
            exclusive: false,
            deadLetterExchange: '',
            deadLetterRoutingKey: 'test.q',
            messageTtl: 1000,
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
         pattern: '',
      },
      {
         exchange: 'example.dead.ex',
         queue: 'example.dead.q',
         pattern: '',
      },
   ],
}
