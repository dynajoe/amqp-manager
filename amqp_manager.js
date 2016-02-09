const Amqp = require('amqplib')
const Util = require('util')
const EventEmitter = require('events').EventEmitter
const Machina = require('machina')
const _ = require('lodash')
const QueryString = require('querystring')
const Logger = require('./logger')

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
               _.assign(this.memory, {
                  connection: connection,
                  reconnects: 0,
               })

               this.transition('connected')
            }, error => {
               this.handle('error', error)
            })
         },
         error: function () {
            Logger.info('connect/error')
            this.deferAndTransition('disconnect')
         },
      },
      connected: {
         _onEnter: function () {
            Logger.info('connected/enter')

            this.memory.connection.on('error', error => {
               this.handle('connection_error', error)
            })

            this.memory.connection.on('close', error => {
               this.handle('connection_close', error)
            })

            this.emit('connected', this.memory.connection)
         },
         connection_error: function (error) {
            Logger.info('connected/connection_error', error)
            this.deferAndTransition('disconnect')
         },
         connection_close: function (error) {
            Logger.info('connected/connection_close', error)
            this.deferAndTransition('disconnect')
         },
      },
      disconnect: {
         _onEnter: function () {
            Logger.info('disconnect/enter')

            if (this.memory.connection) {
               this.memory.connection.removeAllListeners()
            }

            if (this.memory.connection) {
               this.memory.connection.close()
            }

            this.emit('disconnected')

            if (!this.closed) {
               this.transition('reconnect')
            }
         },
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
      this.closed = true
      this.transition('disconnect')
   },
})

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

const AmqpManager = function (config) {
   EventEmitter.call(this)

   this._channnels = {}
   this._connection = null
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

   // Re-establish the topology
   this.fsm.on('connected', connection => {
      Logger.info('manager/topology')
      connection.createChannel()
      .then(channel => {
         channel.prefetch(this.config.channel.prefetch)

         return assertTopology(this.config, channel)
         .then(() => {
            this._channels = {}
            this._connection = connection
            this.emit('ready', connection)
         })
      }, error => {
         this.emit('error', error)
      })
   })

   this.fsm.on('disconnect', () => {
      this._connection = null
      this._channnels = {}
   })

   this.started = false
}

Util.inherits(AmqpManager, EventEmitter)

AmqpManager.prototype.connect = function () {
   if (!this.started) {
      Logger.info('amqpmanager/connect')
      this.started = true
      this.fsm.open({
         config: this.config
      })
   }
}

AmqpManager.prototype.confirmChannel = function (name) {
   return this._channel('createConfirmChannel', name)
}

AmqpManager.prototype.channel = function (name) {
   return this._channel('createChannel', name)
}

AmqpManager.prototype._channel = function (type, name) {
   if (this.closed) {
      return Promise.reject(new Error('Connection closed'))
   }

   const getChannel = (connection) => {
      const key = `${type}:${name || 'default'}`

      if (this._channels[key]) {
         return Promise.resolve(this._channels[key])
      }

      Logger.info('amqpmanager/new-channel', type, name)

      return connection[type]()
      .then(ch => {
         this._channels[key] = ch
         return ch
      })
   }

   if (this._connection) {
      return getChannel(this._connection)
   }

   this.connect()

   const timeout = new Promise((_, reject) => {
      Logger.info('amqpmanager/wait-timeout')

      setTimeout(() => {
         reject(new Error('Channel Timeout'))
      }, 2000)
   })

   if (!this.waitReady) {
      this.waitReady = new Promise(resolve => {
         Logger.info('amqpmanager/wait-ready')

         const onReady = connection => {
            this.removeListener('ready', onReady)
            resolve(connection)
            this.waitReady = null
         }

         this.on('ready', onReady)
      })
   }

   return Promise.race([timeout, this.waitReady.then(c => getChannel(c))])
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
