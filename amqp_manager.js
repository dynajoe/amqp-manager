const Util = require('util')
const EventEmitter = require('events').EventEmitter
const QueryString = require('querystring')
const Logger = require('./logger')
const AmqpConnectionFsm = require('./connection_fsm')
const _ = require('lodash')

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
