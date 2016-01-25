const Logger = require('./logger')

const AmqpBroker = function (amqpManager, config) {
   this.amqpManager = amqpManager
   this.config = config
}

AmqpBroker.prototype.publish = function (msg) {
   Logger.info('broker/publish')

   const data = new Buffer(JSON.stringify(msg))

   return this.amqpManager.channel()
   .then(ch => {
      return ch.publish(this.config.publish.exchange, this.config.publish.pattern, data, this.config.publish.options)
   })
   .then(msg)
}

AmqpBroker.prototype.handle = function (callback) {
   Logger.info('broker/handle')

   var removed = false

   const onDisconnected = () => {
      Logger.info('broker/disconnected')
      this.consumerTag = null
   }

   const onConnected = () => {
      Logger.info('broker/connected')
      if (!this.consumerTag && !removed) {
         startConsuming()
      }
   }

   const addAmqpListeners = () => {
      this.amqpManager.on('connected', onConnected)
      this.amqpManager.on('disconnected', onDisconnected)
   }

   const removeAmqpListeners = () => {
      this.amqpManager.removeListener('disconnected')
      this.amqpManager.removeListener('connected')
   }

   const startConsuming = () => {
      this.amqpManager.channel()
      .then(ch => {
         if (this.consumerTag || removed) {
            return
         }

         Logger.info('broker/startConsuming')

         this.consumerTag = ch.consume(this.config.consume.queue, msg => {
            callback({
               data: JSON.parse(msg.content),
               fields: msg.fields,
               properties: msg.properties,
               ack: () => {
                  ch.ack(msg)
               },
               nack: () => {
                  ch.nack(msg, false, true)
               },
               reject: () => {
                  ch.nack(msg, false, false)
               }
            })
         }, this.config.consume.options)
      })
   }

   const stopConsuming = () => {
      Logger.info('broker/stopConsuming')
      removed = true

      removeAmqpListeners()

      if (this.consumerTag) {
         return this.amqpManager.channel()
         .then(ch => ch.cancel(this.consumerTag))
      }

      return Promise.resolve()
   }

   addAmqpListeners()

   process.nextTick(startConsuming, 1)

   return {
      remove: stopConsuming
   }
}

module.exports = AmqpBroker
