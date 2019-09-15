import { EventEmitter } from 'events'
import { AmqpConnectionFsm } from './connection_fsm'
import * as _ from 'lodash'
import * as Amqp from 'amqplib'
import * as T from './types'
import Debug from 'debug'

const Log = Debug('manager')

export class AmqpManager extends EventEmitter {
   private config: T.AmqpConfig
   private started: boolean = false
   private closed: boolean = false
   private waitReady: Promise<Amqp.Connection> | null
   private fsm: any
   private connection: Amqp.Connection | null
   private channels: _.Dictionary<Amqp.Channel | Amqp.ConfirmChannel>

   constructor(config: T.AmqpConfig) {
      super()

      this.channels = {}
      this.connection = null
      this.waitReady = null
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
      this.fsm.on('connected', (connection: Amqp.Connection) => {
         Log('manager/topology')
         connection.createChannel().then(
            async channel => {
               await this._assertTopology(this.config, channel)
               this.channels = {}
               this.connection = connection
               this.emit('ready', connection)
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
         Log('amqpmanager/connect')
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

   private async _channel(type: 'createConfirmChannel' | 'createChannel', name: string): Promise<Amqp.ConfirmChannel | Amqp.Channel> {
      if (this.closed) {
         throw new Error('Connection closed')
      }

      const getChannel = async (connection: Amqp.Connection): Promise<Amqp.ConfirmChannel | Amqp.Channel> => {
         const key = `${type}:${name || 'default'}`

         if (this.channels[key]) {
            return Promise.resolve(this.channels[key])
         }

         Log('amqpmanager/new-channel', type, name)

         this.channels[key] = await connection[type]()

         return this.channels[key]
      }

      if (this.connection) {
         return getChannel(this.connection)
      }

      this.connect()

      const timeout = new Promise<never>((_, reject) => {
         Log('amqpmanager/wait-timeout')

         setTimeout(() => {
            reject(new Error('Channel Timeout'))
         }, 2000)
      })

      if (!this.waitReady) {
         this.waitReady = new Promise<Amqp.Connection>(resolve => {
            Log('amqpmanager/wait-ready')

            const onReady = (connection: Amqp.Connection) => {
               this.removeListener('ready', onReady)
               resolve(connection)
               this.waitReady = null
            }

            this.on('ready', onReady)
         })
      }

      return Promise.race([timeout, this.waitReady.then(c => getChannel(c))])
   }

   private async _assertTopology(config: T.AmqpConfig, channel: Amqp.Channel): Promise<void> {
      await Promise.all(_.map(config.exchanges, e => channel.assertExchange(e.exchange, e.type, e.options)))
      await Promise.all(_.map(config.queues, q => channel.assertQueue(q.queue, q.options)))
      await Promise.all(_.map(config.bindings, b => channel.bindQueue(b.queue, b.exchange, b.pattern)))
   }
}
