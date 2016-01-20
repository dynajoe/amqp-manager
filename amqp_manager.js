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

const AmqpConnectionFsm = () => {
   return new Machina.BehavioralFsm({
      namespace: 'amqp-connection',
      initialState: 'uninitialized',
      states: {
         uninitialized: {
            start: function (client) {
               this.transition(client, 'connect')
            }
         },
         connect: {
            _onEnter: function (state) {
               const connection = state.config.connection
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

                  channel.on('error', () => {
                     this.transition(nextState, 'failed')
                  })

                  channel.on('close', () => {
                     this.transition(nextState, 'failed')
                  })

                  this.transition(nextState, 'ready')
               }, error => {
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
                  this.transition({
                     config: state.config,
                     reconnects: reconnects + 1,
                  }, 'connect')
               }, waitTimeMs)
            }
         },
         failed: {
            _onEnter: function (state) {
               this.transition(state, 'reconnect')
               this.emit('failed', state)
            }
         },
         ready: {
            _onEnter: function (state) {
               this.emit('ready', state)
            }
         },
         error: {
            _onEnter: function (err) {
               console.log('Error', err)
            }
         },
      },
      start: function (state) {
         this.handle(state, 'start')
      },
      stop: function () {

      }
   })
}

const AmqpManager = function (config) {
   EventEmitter.call(this)
   this.config = config
   this.fsm = AmqpConnectionFsm()

   this.fsm.on('ready', state => {
      this._channel = state.channel
      this.emit('connected')
   })

   this.fsm.on('failed', state => {
      this._channel = null
      this.emit('disconnected')
   })

   this.fsm.start({
      config: this.config
   })
}

Util.inherits(AmqpManager, EventEmitter)

AmqpManager.prototype.channel = function () {
   if (this._channel) {
      return Promise.resolve(this._channel)
   }

   return Promise.reject(null)
}

module.exports = AmqpManager
