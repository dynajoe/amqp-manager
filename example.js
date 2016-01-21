'use-strict';

const AmqpBroker = require('./index')

const Config = {
   AMQP_USER: 'guest',
   AMQP_PASSWORD: 'guest',
   AMQP_HOST: '192.168.99.100',
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
   channel: {
      confirm: true,
      prefetch: 1
   },
   exchanges: [{
      exchange: 'device.inbound.ex',
      type: 'fanout',
      options: { durable: true },
   }, {
      exchange: 'device.inbound.dead.ex',
      type: 'fanout',
      options: { durable: true },
   }, {
      exchange: 'device.sensor.ex',
      type: 'fanout',
      options: { durable: true },
   }, {
      exchange: 'device.sensor.dead.ex',
      type: 'fanout',
      options: { durable: true },
   }],
   queues: [{
      queue: 'device.inbound.q',
      options: {
         durable: true,
         autoDelete: false,
         exclusive: false,
         deadLetterExchange: 'device.inbound.dead.ex',
      }
   }, {
      queue: 'device.sensor.q',
      options: {
         durable: true,
         autoDelete: false,
         exclusive: false,
         deadLetterExchange: 'device.sensor.dead.ex',
      }
   }, {
      queue: 'device.inbound.dead.q',
      options: {
         durable: true,
         autoDelete: false,
         exclusive: false,
      }
   }, {
      queue: 'device.sensor.dead.q',
      options: {
         durable: true,
         autoDelete: false,
         exclusive: false,
      }
   }],
   bindings: [{
      exchange: 'device.inbound.ex',
      queue: 'device.inbound.q',
   }, {
      exchange: 'device.sensor.ex',
      queue: 'device.sensor.q',
   }, {
      exchange: 'device.inbound.dead.ex',
      queue: 'device.inbound.dead.q',
   }, {
      exchange: 'device.sensor.dead.ex',
      queue: 'device.sensor.dead.q',
   }]
}

const RunApp = () => {
   const amqp = AmqpBroker.configure(AmqpConfig)

   amqp.registerBroker('device.inbound', 'device.inbound.ex', 'device.inbound.q', '')
   amqp.registerBroker('device.sensor', 'device.sensor.ex', 'device.sensor.q', '')

   amqp.registrar.broker('device.inbound')
   .then(broker => {
      setInterval(() => {
         broker.publish(new Date().toString())
         .catch(e => {
            console.log(new Date(), 'Error', e.message)
         })
      }, 1000)

      broker.handle(msg => {
         console.log(msg.data)
         msg.ack()
      })
   })
}

RunApp()
