module.exports = {
   info: (msg) => {
      if (process.env.DEBUG) {
         console.log(msg)
      }
   }
}
