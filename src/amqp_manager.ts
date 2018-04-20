import { EventEmitter } from 'events'
import { Logger } from './logger'
import { AmqpConnectionFsm } from './connection_fsm'
import * as _ from 'lodash'
import * as Amqp from 'amqplib'

export interface AmqpExchangeConfig {
   exchange: string
   type: string
   options: Amqp.Options.AssertExchange
}

export interface AmqpQueueConfig {
   queue: string
   options: Amqp.Options.AssertQueue
}

export interface AmqpBindingConfig {
   exchange: string
   queue: string
}

export interface AmqpConnectionConfig {
   user: string
   password: string
   host: string
   port: number
   vhost: string
   protocol?: string
   params?: {
      heartbeat: number
   }
   options: null
}

export interface AmqpConfig {
   connection: AmqpConnectionConfig
   exchanges: AmqpExchangeConfig[]
   queues: AmqpQueueConfig[]
   bindings: AmqpBindingConfig[]
}

export class AmqpManager extends EventEmitter {
   private config: AmqpConfig
   private fsm: any
   private started: boolean
   private closed: boolean
   private waitReady: Promise<any>
   private _channels: {}
   private _connection: null

   constructor(config: AmqpConfig) {
      super()

      this._channels = {}
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
         connection.createChannel().then(
            channel => {
               return this._assertTopology(this.config, channel).then(() => {
                  this._channels = {}
                  this._connection = connection
                  this.emit('ready', connection)
               })
            },
            error => {
               this.emit('error', error)
            }
         )
      })

      this.fsm.on('disconnect', () => {
         this._connection = null
         this._channels = {}
      })

      this.started = false
   }

   connect(): void {
      if (!this.started) {
         Logger.info('amqpmanager/connect')
         this.started = true
         this.fsm.open({ config: this.config })
      }
   }

   async confirmChannel(name): Promise<Amqp.ConfirmChannel> {
      return this._channel('createConfirmChannel', name)
   }

   async channel(name): Promise<Amqp.ConfirmChannel> {
      return this._channel('createChannel', name)
   }

   async _channel(type, name): Promise<any> {
      if (this.closed) {
         throw new Error('Connection closed')
      }

      const getChannel = connection => {
         const key = `${type}:${name || 'default'}`

         if (this._channels[key]) {
            return Promise.resolve(this._channels[key])
         }

         Logger.info('amqpmanager/new-channel', type, name)

         return connection[type]().then(ch => {
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

   async close(): Promise<void> {
      return new Promise<void>(resolve => {
         this.fsm.close()

         const onClosed = () => {
            this.fsm.off('close', onClosed)
            resolve()
         }

         this.fsm.on('close', onClosed)
      })
   }

   _assertTopology(config, channel): Promise<any> {
      const assertExchanges = () => Promise.all(config.exchanges.map(e => channel.assertExchange(e.exchange, e.type, e.options)))

      const assertQueues = () => Promise.all(config.queues.map(q => channel.assertQueue(q.queue, q.options)))

      const bindQueues = () => Promise.all(config.bindings.map(b => channel.bindQueue(b.queue, b.exchange, b.pattern)))

      return assertExchanges()
         .then(assertQueues)
         .then(bindQueues)
   }
}
