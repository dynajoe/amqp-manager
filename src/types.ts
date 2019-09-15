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
   pattern: string
}

export interface AmqpConfig {
   amqplib: {
      connection: Amqp.Options.Connect
      socket_options?: {
         cert: Buffer
         key: Buffer
         passphrase: string
         ca: Buffer[]
      }
   }
   exchanges: AmqpExchangeConfig[]
   queues: AmqpQueueConfig[]
   bindings: AmqpBindingConfig[]
}
