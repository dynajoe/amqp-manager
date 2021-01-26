import { EventEmitter } from 'events'
import { Logger } from './logger'
import { AmqpConnectionFsm } from './connection_fsm'
import * as _ from 'lodash'
import * as Amqp from 'amqplib'
import * as T from './types'

export class AmqpManager extends EventEmitter {
   private config: T.AmqpConfig
   private started: boolean
   private closed: boolean
   private waitReady: Promise<void>
   private fsm: any
   private connection: Amqp.Connection
   private channels: _.Dictionary<Amqp.Channel | Amqp.ConfirmChannel>

   constructor(config: T.AmqpConfig) {
      super()

      this.channels = {}
      this.connection = null
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
         channel_timeout: 2000,
      })

      this.fsm = new AmqpConnectionFsm(this.config)

      // Re-establish the topology
      this.fsm.on('connected', connection => {
         Logger.info('manager/topology')
         connection.createChannel().then(
            channel => {
               return this._assertTopology(this.config, channel).then(() => {
                  this.channels = {}
                  this.connection = connection
                  this.emit('ready', connection)
               })
            },
            error => {
               this.emit('error', error)
            }
         )
      })

      this.fsm.on('disconnect', () => {
         this.connection = null
         this.channels = {}
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

   async confirmChannel(name: string): Promise<Amqp.ConfirmChannel> {
      return <Amqp.ConfirmChannel>await this._channel('createConfirmChannel', name)
   }

   async channel(name: string): Promise<Amqp.Channel> {
      return <Amqp.Channel>await this._channel('createChannel', name)
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

   private async _channel(type: string, name: string): Promise<Amqp.ConfirmChannel | Amqp.Channel> {
      if (this.closed) {
         throw new Error('Connection closed')
      }

      const getChannel = connection => {
         const key = `${type}:${name || 'default'}`

         if (this.channels[key]) {
            return Promise.resolve(this.channels[key])
         }

         Logger.info('amqpmanager/new-channel', type, name)

         return connection[type]().then(ch => {
            this.channels[key] = ch
            return ch
         })
      }

      if (this.connection) {
         return getChannel(this.connection)
      }

      this.connect()

      const timeout = new Promise((_, reject) => {
         Logger.info('amqpmanager/wait-timeout')

         setTimeout(() => {
            reject(new Error('Channel Timeout'))
         }, this.config.channel_timeout)
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

   private async _assertTopology(config, channel): Promise<void> {
      await Promise.all(_.map(config.exchanges, e => channel.assertExchange(e.exchange, e.type, e.options)))
      await Promise.all(_.map(config.queues, q => channel.assertQueue(q.queue, q.options)))
      await Promise.all(_.map(config.bindings, b => channel.bindQueue(b.queue, b.exchange, b.pattern)))
   }
}
