const Amqp = require('amqplib')
const Util = require('util')
const EventEmitter = require('events').EventEmitter
const Machina = require('machina')
const _ = require('lodash')
const QueryString = require('querystring')
const Logger = require('./logger')

const assertTopology = function (config, channel) {
   const assertExchanges = () => Promise.all(
      config.exchanges.map(e => channel.assertExchange(e.exchange, e.type, e.options)))

   const assertQueues = () => Promise.all(
      config.queues.map(q => channel.assertQueue(q.queue, q.options)))

   const bindQueues = () => Promise.all(
      config.bindings.map(b => channel.bindQueue(b.queue, b.exchange, b.pattern)))

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
            Logger.info('connect/enter')
            const connection = this.config.connection
            const amqpUrl = `${connection.protocol}://${connection.user}:${connection.password}@${connection.host}:${connection.port}/${encodeURIComponent(connection.vhost)}?${QueryString.stringify(connection.params)}`

            Amqp.connect(amqpUrl, connection.options)
            .then(connection => {
               connection.on('error', error => {
                  this.handle('connection_error', error)
               })

               connection.on('error', error => {
                  this.handle('connection_close', error)
               })

               _.assign(this.memory, {
                  connection: connection,
                  reconnects: 0,
               })

               this.emit('connected', this.memory)
               this.transition('assert')
            }, error => {
               this.handle('error', error)
            })
         },
         connection_close: function () {
            Logger.info('connect/connection_close')
            this.deferAndTransition('disconnect')
         },
         connection_error: function () {
            Logger.info('connect/connection_error')
            this.deferAndTransition('disconnect')
         },
         error: function () {
            Logger.info('connect/error')
            this.deferAndTransition('disconnect')
         },
      },
      assert: {
         _onEnter: function () {
            Logger.info('assert/enter')
            const channelType = this.config.channel.confirm
               ? 'createConfirmChannel' : 'createChannel'

            this.memory.connection[channelType]()
            .then(channel => {
               channel.prefetch(this.config.channel.prefetch)

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
         connection_close: function () {
            Logger.info('assert/connection_close')
            this.deferAndTransition('disconnect')
         },
         connection_error: function () {
            Logger.info('assert/connection_error')
            this.deferAndTransition('disconnect')
         },
         channel_close: function () {
            Logger.info('assert/channel_close')
            this.deferAndTransition('disconnect')
         },
         channel_error: function () {
            Logger.info('assert/channel_error')
            this.deferAndTransition('disconnect')
         },
      },
      connected: {
         _onEnter: function () {
            Logger.info('connected/enter')
            this.emit('ready', this.memory.channel)
         },
         connection_close: function () {
            Logger.info('connected/connection_close')
            this.deferAndTransition('disconnect')
         },
         connection_error: function () {
            Logger.info('connected/connection_error')
            this.deferAndTransition('disconnect')
         },
         channel_close: function () {
            Logger.info('connected/channel_close')
            this.deferAndTransition('disconnect')
         },
         channel_error: function () {
            Logger.info('connected/channel_error')
            this.deferAndTransition('disconnect')
         },
      },
      disconnect: {
         _onEnter: function () {
            Logger.info('disconnect/enter', this.priorState)
            if (this.memory.connection) {
               this.memory.connection.removeAllListeners()
            }

            if (this.memory.channel) {
               this.memory.channel.removeAllListeners()
            }

            if (this.memory.connection) {
               this.memory.connection.close()
            }

            this.emit('disconnected')
         },
         '*': function () {
            Logger.info('disconnect/*')
         },
         error: function () {
            Logger.info('disconnect/error')
            this.transition('reconnect')
         },
         connection_error: function () {
            Logger.info('disconnect/connection_error')
            this.transition('reconnect')
         },
         connection_close: function () {
            Logger.info('disconnect/connection_error')
            this.transition('reconnect')
         },
         channel_close: function () {
            Logger.info('disconnect/channel_close')
            this.transition('reconnect')
         },
         channel_error: function (error) {
            Logger.info('disconnect/channel_error')
            if (/PRECONDITION-FAILED/i.test(error.message)) {
               return this.emit('error', error)
            }

            this.transition('reconnect')
         }
      },
      reconnect: {
         _onEnter: function () {
            Logger.info('reconnect/enter')
            const reconnects = (this.memory.reconnects || 0) + 1
            const waitTimeMs = Math.min(Math.pow(2, reconnects) * 100, 60 * 1000)

            this.emit('reconnect_waiting', {
               reconnects: reconnects,
               wait_time_ms: waitTimeMs
            })

            setTimeout(() => {
               this.emit('reconnecting', {
                  reconnects: reconnects,
                  wait_time_ms: waitTimeMs
               })

               _.assign(this.memory, {
                  reconnects: reconnects,
               })

               this.transition('connect')
            }, waitTimeMs)
         }
      },
   },
   open: function () {
      this.handle('open')
   },
   close: function () {
      this.transition('disconnect')
   },
})

const AmqpManager = function (config) {
   EventEmitter.call(this)

   this.config = _.defaultsDeep(config, {
      connection: {
         protocol: 'amqp',
         user: 'guest',
         password: 'guest',
         host: 'localhost',
         port: 5672,
         vhost: '/',
         params: {
            heartbeat: 30,
         },
         options: null,
      },
      channel: {
         prefetch: 1,
         confirm: true,
      },
      queues: [],
      exchanges: [],
      bindings: [],
   })

   this.fsm = new AmqpConnectionFsm(this.config)

   this.fsm.on('ready', channel => {
      this._channel = channel
      this.emit('connected')
   })

   this.fsm.on('reconnect_waiting', info => {
      this._channel = null
      this.emit('reconnect_waiting', info)
   })

   this.fsm.on('reconnecting', info => {
      this._channel = null
      this.emit('reconnecting', info)
   })

   this.fsm.on('close', () => {
      this.closed = true
      this._channel = null
      this.emit('disconnected')
   })

   this.fsm.on('disconnected', () => {
      this._channel = null
      this.emit('disconnected')
   })

   this.fsm.on('error', () => {
      this._channel = null
      this.emit('error')
   })

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
      const onReady = channel => {
         this.fsm.off('ready', onReady)
         resolve(channel)
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
