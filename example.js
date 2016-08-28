'use strict';

const AmqpManager = require('./index')
const Logger = require('./logger')

const Config = {
   AMQP_USER: 'guest',
   AMQP_PASSWORD: 'guest',
   AMQP_HOST: '127.0.0.1',
   AMQP_PORT: '5672',
   AMQP_VHOST: '/',
}

const AmqpConfig = {
   connection: {
      user: Config.AMQP_USER,
      password: Config.AMQP_PASSWORD,
      host: Config.AMQP_HOST,
      port: Config.AMQP_PORT,
      vhost: Config.AMQP_VHOST,
   },
   exchanges: [{
      exchange: 'example.ex',
      type: 'fanout',
      options: { durable: true },
   }, {
      exchange: 'example.dead.ex',
      type: 'fanout',
      options: { durable: true },
   }],
   queues: [{
      queue: 'test.q',
      options: {
         durable: true,
         autoDelete: false,
         exclusive: false,
         deadLetterExchange: 'example.dead.ex',
      }
   }, {
      queue: 'example.dead.q',
      options: {
         durable: true,
         autoDelete: false,
         exclusive: false,
      }
   }],
   bindings: [{
      exchange: 'example.ex',
      queue: 'test.q',
   }, {
      exchange: 'example.dead.ex',
      queue: 'example.dead.q',
   }]
}

const RunApp = () => {
   const amqpManager = new AmqpManager(AmqpConfig)

   let consumerTag = null

   amqpManager.on('ready', () => {
      amqpManager.channel('inbound')
      .then(ch => {
         ch.prefetch(1)

         consumerTag = ch.consume('test.q', msg => {
            console.log(JSON.parse(msg.content))
            ch.reject(msg, false)
         })
      })
   })

   setInterval(() => {
      Logger.info('publishing')
      amqpManager.confirmChannel('outbound')
      .then(ch => {
         ch.publish('example.ex', '', new Buffer(JSON.stringify({
            date: new Date()
         })))

         ch.waitForConfirms()
      })
      .catch(e => {
         console.log(new Date(), 'Error', e)
         console.log(e.stack)
      })
   }, 1000)

   amqpManager.connect()
}

RunApp()
