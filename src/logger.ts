export const Logger = {
   info: (message: string, ...args: any[]) => {
      if (process.env.DEBUG) {
         console.log(message, Array.prototype.slice.call(args))
      }
   },
}
