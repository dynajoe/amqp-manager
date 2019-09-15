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

export interface SubscribeOptions<T> {
   prefetch_count: number
   queue: string
   channel_name: string
   retry_queue: string
   dead_letter_queue: string
   dead_letter_exchange: string
   max_retry_count: number
   handler_timeout_ms: number
   handler: Handler<T>
   parser: Parser<T>
   onError(error: any): void
}

export interface AckInput<T> {
   message: T
   raw: Amqp.ConsumeMessage
   ack(): Promise<void>
   nack(): Promise<void>
   reject(): Promise<void>
   isHandled(): boolean
}

export interface Parser<T> {
   (message: Amqp.Message): Promise<T>
}

export interface Handler<T> {
   (input: AckInput<T>): Promise<void>
}

export interface Subscription {
   cancel(): Promise<void>
}

export interface PublishOptions {
   exchange: string
   queue?: string
   confirm: boolean
   data: Buffer
   amqp_options?: Amqp.Options.Publish
}
