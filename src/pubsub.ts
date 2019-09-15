import * as Amqp from 'amqplib'
import { AmqpManager } from './amqp_manager'
import * as T from './types'
import * as _ from 'lodash'
import Debug from 'debug'

const Log = Debug('amqp-manager:pubsub')

/*
   In the event of a synchronous unhandled exception we want to Reject the
   message to ensure that we don't get into a spin fail situation. If a promise is
   provided with the handler any UNHANDLED promise failures will also reject. Handlers SHOULD
   handle all errors and ack/nack/reject as appropriate. However, in the event that they aren't handling appropriately
   this protects us from 100% CPU usage :)

   Otherwise, unhandled exceptions will propagate and kill the AMQP connection.
*/
function newMessageHandler<T>(
   channel: Amqp.Channel | Amqp.ConfirmChannel,
   amqp_manager: AmqpManager,
   subscribe_options: T.SubscribeOptions<T>
): (m: Amqp.ConsumeMessage | null) => void {
   return async (message: Amqp.ConsumeMessage | null) => {
      if (Log.enabled) {
         Log('message_received [%o]', _.isNil(message) ? null : { ...message, content: `<<${message.content.length} bytes>>` })
      }

      if (_.isNil(message)) {
         return
      }

      try {
         const parsed_message = await subscribe_options.parser(message)

         const input = asAckInput(channel, amqp_manager, message, parsed_message, subscribe_options)

         let timeout_handle: any

         const timeout = new Promise(resolve => {
            timeout_handle = setTimeout(() => resolve(), _.defaultTo(subscribe_options.handler_timeout_ms, 60000))
         })

         const onComplete = async () => {
            clearTimeout(timeout_handle)

            if (!input.isHandled()) {
               await input.reject()
            }
         }

         try {
            await Promise.race([subscribe_options.handler(input), timeout])

            await onComplete()
         } catch (error) {
            await onComplete()
            throw error
         }
      } catch (error) {
         if (_.isNil(subscribe_options.retry_queue)) {
            channel.reject(message, false)
         } else {
            await retryOrBackoff(channel, amqp_manager, subscribe_options, message)
         }
      }
   }
}

function asAckInput<T>(
   channel: Amqp.Channel | Amqp.ConfirmChannel,
   amqp_manager: AmqpManager,
   message: Amqp.Message,
   parsed_message: T,
   subscribe_options: T.SubscribeOptions<T>
): T.AckInput<T> {
   let is_message_handled = false

   return {
      message: parsed_message,
      raw: message,
      isHandled: () => is_message_handled,
      async ack() {
         if (is_message_handled) {
            return
         }

         is_message_handled = true

         channel.ack(message, false)
      },
      async nack() {
         if (is_message_handled) {
            return
         }

         is_message_handled = true

         if (_.isNil(subscribe_options.retry_queue)) {
            channel.nack(message)
         } else {
            await retryOrBackoff(channel, amqp_manager, subscribe_options, message)
         }
      },
      async reject() {
         if (is_message_handled) {
            return
         }

         is_message_handled = true

         if (_.isNil(subscribe_options.retry_queue)) {
            channel.reject(message, false)
         } else {
            await retryOrBackoff(channel, amqp_manager, subscribe_options, message)
         }
      },
   }
}

async function retryOrBackoff<T>(
   channel: Amqp.Channel | Amqp.ConfirmChannel,
   amqp_manager: AmqpManager,
   queue_config: T.SubscribeOptions<T>,
   message: Amqp.Message
): Promise<any> {
   const message_retry_count: number = _.get(message, 'properties.headers.x-death[0].count', 0)

   if (Log.enabled) {
      Log(
         'retry_or_backoff [retry_count=%d,max_retries=%d,dlx=%s,dlq=%s]',
         message_retry_count,
         queue_config.max_retry_count,
         queue_config.dead_letter_exchange,
         queue_config.dead_letter_queue
      )
   }

   if (message_retry_count >= queue_config.max_retry_count) {
      try {
         await publish(amqp_manager, {
            confirm: true,
            data: message.content,
            exchange: queue_config.dead_letter_exchange,
            queue: queue_config.dead_letter_queue,
         })
      } finally {
         channel.ack(message)
      }
   } else {
      channel.nack(message, false, false)
   }
}

export function subscribe<T>(amqp_manager: AmqpManager, subscribe_options: T.SubscribeOptions<T>): T.Subscription {
   if (Log.enabled) {
      Log('subscribe [%o]', subscribe_options)
   }

   let consumer_tag: string | null = null

   amqp_manager.connect()

   const { channel_name, queue } = subscribe_options

   amqp_manager.on('ready', async () => {
      try {
         const channel: Amqp.Channel = await amqp_manager.channel(channel_name)

         await channel.prefetch(subscribe_options.prefetch_count)

         const consume_reply = await channel.consume(queue, newMessageHandler<T>(channel, amqp_manager, subscribe_options))

         consumer_tag = consume_reply.consumerTag
      } catch (error) {
         subscribe_options.onError(error)
      }
   })

   return {
      async cancel(): Promise<void> {
         Log('cancel_subscription')

         if (!_.isNil(consumer_tag)) {
            const channel = await amqp_manager.channel(channel_name)
            await channel.cancel(consumer_tag)
         }
      },
   }
}

export async function publish(amqp_manager: AmqpManager, publish_options: T.PublishOptions): Promise<void> {
   if (Log.enabled) {
      Log('publish [%o]', { ...publish_options, data: `<<${publish_options.data.length} bytes>>` })
   }

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
