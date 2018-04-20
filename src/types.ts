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
   socket_options?: {
      cert: Buffer
      key: Buffer
      passphrase: string
      ca: Buffer[]
   }
}

export interface AmqpConfig {
   connection: AmqpConnectionConfig
   exchanges: AmqpExchangeConfig[]
   queues: AmqpQueueConfig[]
   bindings: AmqpBindingConfig[]
}
