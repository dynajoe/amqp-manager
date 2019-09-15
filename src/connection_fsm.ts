import * as _ from 'lodash'
import * as Amqp from 'amqplib'
import * as T from './types'
import Debug from 'debug'

const Log = Debug('amqp-manager:connection')
const Machina = require('machina')

export const AmqpConnectionFsm = Machina.Fsm.extend({
   namespace: 'amqp-connection',
   initialState: 'uninitialized',
   initialize: function(config: T.AmqpConfig) {
      this.config = config
      this.memory = {}
   },
   states: {
      uninitialized: {
         '*': function() {
            this.deferAndTransition('idle')
         },
      },
      idle: {
         open: function() {
            this.transition('connect')
         },
      },
      connect: {
         _onEnter: function() {
            Log('connect/enter')
            const config: T.AmqpConfig = this.config
            const amqplib_config = config.amqplib

            Amqp.connect(amqplib_config.connection, amqplib_config.socket_options).then(
               connection => {
                  _.assign(this.memory, {
                     connection: connection,
                     reconnects: 0,
                  })

                  this.transition('connected')
               },
               error => {
                  this.handle('error', error)
               }
            )
         },
         error: function() {
            Log('connect/error')
            this.deferAndTransition('disconnect')
         },
      },
      connected: {
         _onEnter: function() {
            Log('connected/enter')

            this.memory.connection.on('error', (error: Error) => {
               this.handle('connection_error', error)
            })

            this.memory.connection.on('close', (error: Error) => {
               this.handle('connection_close', error)
            })

            this.emit('connected', this.memory.connection)
         },
         connection_error: function(error: Error & { code: number }) {
            Log('connected/connection_error', error)

            if (error.code === 406) {
               this.deferAndTransition('fatal')
            } else {
               this.deferAndTransition('disconnect')
            }
         },
         connection_close: function(error: Error) {
            Log('connected/connection_close', error)
            this.deferAndTransition('disconnect')
         },
      },
      fatal: {
         _onEnter: function() {
            Log('fatal/enter')

            if (this.memory.connection) {
               this.memory.connection.removeAllListeners()
            }

            if (this.memory.connection) {
               this.memory.connection.close()
            }

            this.emit('fatal')
         },
      },
      disconnect: {
         _onEnter: function() {
            Log('disconnect/enter')

            if (this.memory.connection) {
               this.memory.connection.removeAllListeners()
            }

            if (this.memory.connection) {
               this.memory.connection.close()
            }

            this.emit('disconnected')

            if (!this.closed) {
               this.transition('reconnect')
            }
         },
      },
      reconnect: {
         _onEnter: function() {
            Log('reconnect/enter')
            const reconnects = (this.memory.reconnects || 0) + 1
            const waitTimeMs = Math.min(Math.pow(2, reconnects) * 100, 60 * 1000)

            this.emit('reconnect_waiting', {
               reconnects: reconnects,
               wait_time_ms: waitTimeMs,
            })

            setTimeout(() => {
               this.emit('reconnecting', {
                  reconnects: reconnects,
                  wait_time_ms: waitTimeMs,
               })

               _.assign(this.memory, {
                  reconnects: reconnects,
               })

               this.transition('connect')
            }, waitTimeMs)
         },
      },
   },
   open: function() {
      this.handle('open')
   },
   close: function() {
      this.closed = true
      this.transition('disconnect')
   },
})
