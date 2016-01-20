const Amqp = require('amqplib')
const Util = require('util')
const EventEmitter = require('events').EventEmitter
const Machina = require('machina')
const _ = require('lodash')

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

const AmqpConnectionFsm = Machina.BehavioralFsm.extend({
   namespace: 'amqp-connection',
   initialState: 'uninitialized',
   states: {
      uninitialized: {
         '*': function (state) {
            this.deferAndTransition(state, 'idle')
         },
      },
      idle: {
         start: function (client) {
            this.transition(client, 'connect')
         },
      },
      connect: {
         _onEnter: function (state) {
            const connection = state.config.connection
            // TODO: Allow for secure connection
            const amqpUrl = `amqp://${connection.user}:${connection.password}@${connection.host}:${connection.port}/${encodeURIComponent(connection.vhost)}?heartbeat=30`

            Amqp.connect(amqpUrl)
            .then(connection => {
               this.transition({
                  connection: connection,
                  reconnects: 0,
                  config: state.config,
               }, 'connected')
            }, error => {
               this.transition(_.assign(state, {
                  error: error,
               }), 'failed')
            })
         }
      },
      connected: {
         _onEnter: function (state) {
            // TODO: Any other connection events to consider?
            state.connection.on('error', () => {
               this.transition(state, 'failed')
            })

            this.transition(state, 'assert_topology')
         }
      },
      assert_topology: {
         _onEnter: function (state) {
            assertTopology(state.config, state.connection)
            .then(channel => {
               const nextState = _.assign(state, {
                  channel: channel,
               })

               // TODO: Any other channel events to consider?
               channel.on('error', () => {
                  this.transition(nextState, 'failed')
               })

               // TODO: what if closed intentionally?
               channel.on('close', () => {
                  this.transition(nextState, 'failed')
               })

               this.transition(nextState, 'ready')
            }, error => {
               // TODO: If the topology is bad reconnecting won't help
               this.transition(_.assign(state, {
                  error: error,
               }), 'failed')
            })
         },
      },
      reconnect: {
         _onEnter: function (state) {
            const reconnects = (state.reconnects || 0)
            const waitTimeMs = Math.min(Math.pow(2, reconnects) * 100, 60 * 1000)

            setTimeout(() => {
               // TODO: Don't transition to connect if stopped
               this.transition({
                  config: state.config,
                  reconnects: reconnects + 1,
               }, 'connect')
            }, waitTimeMs)
         }
      },
      failed: {
         _onEnter: function (state) {
            if (state.connection) {
               state.connection.removeAllListeners()
            }

            if (state.channel) {
               state.channel.removeAllListeners()
            }

            this.transition(state, 'reconnect')
            this.emit('failed', state)
         }
      },
      ready: {
         _onEnter: function (state) {
            this.emit('ready', state)
         }
      },
   },
   start: function (state) {
      this.handle(state, 'start')
   },
   stop: function () {
      this.handle({}, 'stop')
   }
})


const AmqpManager = function (config) {
   EventEmitter.call(this)
   this.config = config

   this.fsm = new AmqpConnectionFsm()

   this.fsm.on('ready', state => {
      this._channel = state.channel
      this.emit('connected')
   })

   this.fsm.on('failed', () => {
      if (this._channel) {
         this._channel = null
         this.emit('disconnected')
      }
   })

   this.started = false
}

Util.inherits(AmqpManager, EventEmitter)

AmqpManager.prototype.channel = function () {
   if (this._channel) {
      return Promise.resolve(this._channel)
   }

   if (!this.started) {
      this.fsm.start({
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

AmqpManager.prototype.stop = function () {
   return new Promise((resolve) => {
      this.fsm.stop()
   })
}

module.exports = AmqpManager
