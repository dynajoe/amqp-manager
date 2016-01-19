const Amqp = require('amqplib')
const Util = require('util')
const EventEmitter = require('events').EventEmitter

const assertTopology = function (config, connection) {
   return connection.createChannel()
   .then(channel => {
      const assertExchanges = () => Promise.all(
         config.exchanges.map(e => channel.assertExchange(e.exchange, e.type, e.options)))

      const assertQueues = () => Promise.all(
         config.queues.map(q => channel.assertQueue(q.queue, q.options)))

      const bindQueues = () => Promise.all(
         config.queues.map(q => channel.bindQueue(q.queue, q.options.exchange, q.options.pattern || '')))

      return assertExchanges()
      .then(assertQueues)
      .then(bindQueues)
      .then(() => channel)
   })
}

const AmqpManager = function (config) {
   EventEmitter.call(this)
   this.config = config
}

Util.inherits(AmqpManager, EventEmitter)

AmqpManager.prototype.channel = function () {
   if (this.setupResult) {
      return this.setupResult
   }

   const onClose = (ch) => {
      this.emit('disconnected')
      this.setupResult = null
   }

   const connection = this.config.connection
   const url = `amqp://${connection.user}:${connection.password}@${connection.host}:${connection.port}/${encodeURIComponent(connection.vhost)}?heartbeat=30`

   return this.setupResult = Amqp.connect(url)
   .then(conn => assertTopology(this.config, conn))
   .then(ch => {
      ch.on('close', onClose.bind(null, ch))
      ch.on('error', onClose.bind(null, ch))

      this.emit('connected')
      return ch
   })
}

module.exports = AmqpManager
