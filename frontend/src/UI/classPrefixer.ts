const getClassPrefixer = (rawPrefix: string) => {
  const prefix = rawPrefix.replace(/\W+/g, '')
  return new Proxy(
    {},
    {
      get: (target: any, prop: any) => {
        target[prop] ??= `${prefix}-${prop}`
        return target[prop]
      },
    },
  )
}

export default getClassPrefixer