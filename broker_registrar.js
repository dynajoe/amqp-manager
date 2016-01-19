const BrokerRegistrar = function () {
   this.brokerFns = {}
}

BrokerRegistrar.prototype.broker = function (messageType) {
   if (this.brokerFns[messageType]) {
      return Promise.resolve()
      .then(this.brokerFns[messageType])
   }

   return Promise.reject(new Error(`No message broker registered for type: [${messageType}]`))
}

BrokerRegistrar.prototype.register = function (messageType, brokerFn) {
   this.brokerFns[messageType] = brokerFn
}

module.exports = BrokerRegistrar
