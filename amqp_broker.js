const AmqpBroker = function (amqpManager, config) {
   this.amqpManager = amqpManager
   this.config = config
}

AmqpBroker.prototype.publish = function (msg) {
   const data = new Buffer(JSON.stringify(msg))

   return this.amqpManager.channel()
   .then(ch => {
      return ch.publish(this.config.publish.exchange, this.config.publish.pattern, data, this.config.publish.options)
   })
   .then(null)
}

AmqpBroker.prototype.handle = function (callback) {
   var removed = false

   const onDisconnected = () => {
      this.consumerTag = null
   }

   const onConnected = () => {
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
         if (removed) {
            return
         }

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
      removed = true

      removeAmqpListeners()

      if (this.consumerTag) {
         return this.amqpManager.channel()
         .then(ch => ch.cancel(this.consumerTag))
      }

      return Promise.resolve()
   }

   addAmqpListeners()
   startConsuming()

   return {
      remove: stopConsuming
   }
}

module.exports = AmqpBroker
