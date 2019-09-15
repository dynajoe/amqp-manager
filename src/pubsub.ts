import * as Amqp from 'amqplib'
import { AmqpManager } from './amqp_manager'
import * as _ from 'lodash'

const HANDLE_MESSAGE_TIMEOUT_MS = 60000

/*
   In the event of a synchronous unhandled exception we want to Reject the
   message to ensure that we don't get into a spin fail situation. If a promise is
   provided with the handler any UNHANDLED promise failures will also reject. Handlers SHOULD
   handle all errors and ack/nack/reject as appropriate. However, in the event that they aren't handling appropriately
   this protects us from 100% CPU usage :)

   Otherwise, unhandled exceptions will propagate and kill the AMQP connection.
*/
function NewMessageHandler<T>(
   ch: Amqp.Channel | Amqp.ConfirmChannel,
   amqp_manager: AmqpManager,
   consume_options: SubscribeOptions<T>
): (m: Amqp.ConsumeMessage | null) => void {
   return async (message: Amqp.ConsumeMessage | null) => {
      if (_.isNil(message)) {
         return
      }

      const parsed_message = await consume_options.parser(message)

      try {
         const input = AsAckInput(ch, amqp_manager, message, parsed_message, consume_options)

         let timeout_handle: any

         const timeout = new Promise(resolve => {
            timeout_handle = setTimeout(() => resolve(), HANDLE_MESSAGE_TIMEOUT_MS)
         })

         const onComplete = async () => {
            clearTimeout(timeout_handle)

            if (!input.isHandled()) {
               await input.reject()
            }
         }

         try {
            await Promise.race([consume_options.handler(input, message, ch), timeout])

            await onComplete()
         } catch (error) {
            await onComplete()
            throw error
         }
      } catch (error) {
         if (_.isNil(consume_options.retry_queue)) {
            ch.reject(message, false)
         } else {
            await RetryOrBackoff(ch, amqp_manager, message, consume_options)
         }
      }
   }
}

async function RetryOrBackoff<T>(
   ch: Amqp.Channel | Amqp.ConfirmChannel,
   amqp_manager: AmqpManager,
   message: Amqp.Message,
   queue_config: SubscribeOptions<T>
): Promise<any> {
   const message_retry_count: number = _.get(message, 'properties.headers.x-death[0].count', 0)

   if (message_retry_count >= queue_config.max_retry_count) {
      try {
         await publish(amqp_manager, {
            confirm: true,
            data: message.content,
            exchange: queue_config.dead_letter_exchange,
            queue: queue_config.dead_letter_queue,
         })
      } finally {
         ch.ack(message)
      }
   } else {
      ch.nack(message, false, false)
   }
}

function AsAckInput<T>(
   channel: Amqp.Channel | Amqp.ConfirmChannel,
   amqp_manager: AmqpManager,
   message: Amqp.Message,
   parsed_message: T,
   subscribe_options: SubscribeOptions<T>
): AckInput<T> {
   let handled = false

   return {
      message: parsed_message,
      isHandled: () => handled,
      async ack() {
         if (handled) {
            return
         }

         handled = true

         channel.ack(message, false)
      },
      async nack() {
         if (handled) {
            return
         }

         handled = true

         if (_.isNil(subscribe_options.retry_queue)) {
            channel.nack(message)
         } else {
            await RetryOrBackoff(channel, amqp_manager, message, subscribe_options)
         }
      },
      async reject() {
         if (handled) {
            return
         }

         handled = true

         if (_.isNil(subscribe_options.retry_queue)) {
            channel.reject(message, false)
         } else {
            await RetryOrBackoff(channel, amqp_manager, message, subscribe_options)
         }
      },
   }
}

export interface SubscribeOptions<T> {
   prefetch_count: number
   queue: string
   channel_name: string
   retry_queue: string
   dead_letter_queue: string
   dead_letter_exchange: string
   max_retry_count: number
   parser: Parser<T>
   handler: Handler<T>
   onError(error: any): void
}

export interface AckInput<T> {
   message: T
   ack(): Promise<void>
   nack(): Promise<void>
   reject(): Promise<void>
   isHandled(): boolean
}

export interface Parser<T> {
   (message: Amqp.Message): Promise<T>
}

export interface Handler<T> {
   (input: AckInput<T>, raw_message: Amqp.Message, channel: Amqp.Channel): Promise<void>
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

export function subscribe<T>(amqp_manager: AmqpManager, subscribe_options: SubscribeOptions<T>): Subscription {
   let consumer_tag: string | null = null

   amqp_manager.connect()

   const { channel_name, queue } = subscribe_options

   amqp_manager.on('ready', async () => {
      try {
         const channel: Amqp.Channel = await amqp_manager.channel(channel_name)

         await channel.prefetch(subscribe_options.prefetch_count)

         const consume_reply = await channel.consume(queue, NewMessageHandler<T>(channel, amqp_manager, subscribe_options))

         consumer_tag = consume_reply.consumerTag
      } catch (error) {
         subscribe_options.onError(error)
      }
   })

   return {
      async cancel(): Promise<void> {
         if (!_.isNil(consumer_tag)) {
            const channel = await amqp_manager.channel(channel_name)
            await channel.cancel(consumer_tag)
         }
      },
   }
}

export async function publish(amqp_manager: AmqpManager, publish_options: PublishOptions): Promise<void> {
   const { exchange, queue, confirm, data, amqp_options } = publish_options
   const channel_name = `${publish_options.exchange}-outbound`

   if (confirm) {
      const channel = await amqp_manager.confirmChannel(channel_name)

      await new Promise((resolve, reject) => {
         channel.publish(exchange, _.defaultTo(queue, ''), data, amqp_options, error => {
            return !_.isNil(error) ? reject(error) : resolve()
         })
      })

      await channel.waitForConfirms()
   } else {
      const channel = await amqp_manager.channel(channel_name)
      channel.publish(exchange, _.defaultTo(queue, ''), data, amqp_options)
   }
}
