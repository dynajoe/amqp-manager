const Amqp = require('amqplib')
const Util = require('util')
const EventEmitter = require('events').EventEmitter
const Machina = require('machina')
const _ = require('lodash')

const assertTopology = function (config, channel) {
   const assertExchanges = () => Promise.all(
      config.exchanges.map(e => channel.assertExchange(e.exchange, e.type, e.options)))

   const assertQueues = () => Promise.all(
      config.queues.map(q => channel.assertQueue(q.queue, q.options)))

   const bindQueues = () => Promise.all(
      config.queues.map(q => channel.bindQueue(q.queue, q.options.exchange, q.options.pattern || '')))

   return assertExchanges()
   .then(assertQueues)
   .then(bindQueues)
}

const AmqpConnectionFsm = Machina.Fsm.extend({
   namespace: 'amqp-connection',
   initialState: 'uninitialized',
   initialize: function (config) {
      this.config = config
      this.memory = {}
   },
   states: {
      uninitialized: {
         '*': function () {
            this.deferAndTransition('idle')
         },
      },
      idle: {
         open: function () {
            this.transition('connect')
         },
      },
      connect: {
         _onEnter: function () {
            const connection = this.config.connection
            // TODO: Allow for secure connection
            const amqpUrl = `amqp://${connection.user}:${connection.password}@${connection.host}:${connection.port}/${encodeURIComponent(connection.vhost)}?heartbeat=30`

            Amqp.connect(amqpUrl)
            .then(connection => {
               connection.on('error', error => {
                  this.handle('connection_error', error)
               })

               _.assign(this.memory, {
                  connection: connection,
                  reconnects: 0,
               })

               this.transition('assert')
            }, error => {
               this.handle('error', error)
            })
         },
         connection_error: function () {
            this.transition('reconnect')
         },
         error: function () {
            this.transition('reconnect')
         }
      },
      assert: {
         _onEnter: function () {
            this.memory.connection.createChannel()
            .then(channel => {
               _.assign(this.memory, {
                  channel: channel,
               })

               channel.on('error', error => {
                  this.handle('channel_error', error)
               })

               channel.on('close', close => {
                  this.handle('channel_close', close)
               })

               return assertTopology(this.config, channel)
               .then(() => {
                  this.transition('connected')
               })
            }, error => {
               this.handle('channel_error', error)
            })
         },
         connection_error: function () {
            this.transition('reconnect')
         },
         channel_close: function () {
            this.transition('reconnect')
         },
         channel_error: function (error) {
            if (/PRECONDITION-FAILED/i.test(error.message)) {
               return this.emit('error', error)
            }

            this.transition('reconnect')
         }
      },
      connected: {
         _onEnter: function () {
           this.emit('ready', this.memory)
         },
         channel_error: function () {
            this.transition('reconnect')
         },
         connection_error: function () {
            this.transition('reconnect')
         },
         channel_close: function () {
            this.transition('reconnect')
         }
      },
      reconnect: {
         _onEnter: function () {
            const reconnects = (this.memory.reconnects || 0) + 1
            const waitTimeMs = Math.min(Math.pow(2, reconnects) * 100, 60 * 1000)

            this.emit('reconnect', {
               reconnects: reconnects,
               wait_time_ms: waitTimeMs
            })

            setTimeout(() => {
               // TODO: Don't transition to connect if stopped
               _.assign(this.memory, {
                     reconnects: reconnects,
               })
               this.transition('connect')
            }, waitTimeMs)
         }
      },
      failed: {
         _onEnter: function () {
            this.cleanHandlers()
            this.transition('reconnect')
            this.emit('failed', this.memory)
         }
      },
      error: {
         _onEnter: function (error) {
            this.cleanHandlers()
            this.emit('error', error)
         }
      },
      ready: {
         _onEnter: function () {
            this.emit('ready', this.memory)
         }
      },
      close: {
         _onEnter: function () {
            this.cleanHandlers()

            if (this.memory.connection) {
               this.memory.connection.close()
            }

            this.emit('close')
         }
      }
   },
   open: function () {
      this.handle('open')
   },
   close: function () {
      this.transition('close')
   },
   cleanHandlers: function () {
      if (this.memory.connection) {
         this.memory.connection.removeAllListeners()
      }

      if (this.memory.channel) {
         this.memory.channel.removeAllListeners()
      }
   },
})

const AmqpManager = function (config) {
   EventEmitter.call(this)
   this.config = config

   this.fsm = new AmqpConnectionFsm(this.config)

   this.fsm.on('ready', state => {
      this._channel = state.channel
      this.emit('connected')
   })

   const disconnect = () => {
      if (this._channel) {
         this._channel = null
         this.emit('disconnected')
      }
   }

   this.fsm.on('close', () => {
      this.closed = true
      disconnect()
   })

   this.fsm.on('reconnect', disconnect)

   this.fsm.on('error', disconnect)

   this.started = false
}

Util.inherits(AmqpManager, EventEmitter)

AmqpManager.prototype.channel = function () {
   if (this.closed) {
      return Promise.reject(new Error('Connection closed'))
   }

   if (this._channel) {
      return Promise.resolve(this._channel)
   }

   if (!this.started) {
      this.started = true
      this.fsm.open({
         config: this.config
      })
   }

   const timeout = new Promise((_, reject) => {
      setTimeout(() => {
         reject(new Error('Channel Timeout'))
      }, 2000)
   })

   const waitChannel = new Promise(resolve => {
      const onReady = state => {
         this.fsm.off('ready', onReady)
         resolve(state.channel)
      }

      this.fsm.on('ready', onReady)
   })

   return Promise.race([timeout, waitChannel])
}

AmqpManager.prototype.close = function () {
   return new Promise((resolve) => {
      this.fsm.close()

      const onClosed = () => {
         this.fsm.off('close', onClosed)
         resolve()
      }

      this.fsm.on('close', onClosed)
   })
}

module.exports = AmqpManager
