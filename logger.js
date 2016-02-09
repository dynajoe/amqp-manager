module.exports = {
   info: function () {
      if (process.env.DEBUG) {
         console.log(Array.prototype.slice.call(arguments))
      }
   }
}
