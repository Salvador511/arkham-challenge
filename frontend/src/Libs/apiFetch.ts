import { useMutation, useQuery, useQueryClient, useInfiniteQuery } from '@tanstack/react-query'

export const STALE_TIME_5M = 1000 * 60 * 5
export const STALE_TIME_1M = 1000 * 60

type Methods = 'GET' | 'POST' | 'PUT' | 'PATCH' | 'DELETE'

export const apiFetch = async ({
  payload,
  method,
  url,
}: {
  payload?: any,
  method: Methods,
  url: string,
}) => {
  const options: any = {
    method,
    headers: {
      'Content-Type': 'application/json',
    },
  }
  if (method !== 'GET') {
    options.body = JSON.stringify(payload)
  }
  const response = await fetch(url, options)

  if (!response.ok) {
    let parsedError: any = null

    try {
      parsedError = await response.json()
    } catch {
      parsedError = null
    }

    throw {
      status: response.status,
      statusText: response.statusText,
      message: parsedError?.message
        || parsedError?.detail
        || `API Error: ${response.status} ${response.statusText}`,
      url,
      ...parsedError,
    }
  }

  const data = await response.json()
  return data
}


export const useApiQuery = (
  { url, payload = {}, key, ...queryOptions }:
  { url: string, payload?: any, key: string | readonly unknown[] } & Record<string, any>
) => {
  return useQuery({
    ...queryOptions,
    staleTime: STALE_TIME_5M,
    queryKey: Array.isArray(key) ? [...key] : [key],
    queryFn: () => apiFetch({ url, payload, method: 'GET' }),
  })
}

export const useApiInfiniteQuery = (
  {
    url,
    key,
    limit = 100,
    dataset,
    date_from,
    date_to,
    facility_id,
    ...options
  }:
  {
    url: string,
    key: string,
    limit?: number,
    dataset?: 'facility' | 'us',
    date_from?: string,
    date_to?: string,
    facility_id?: string,
  } & Record<string, any>
) => {
  // Construir query params
  const buildUrl = (offset: number) => {
    const params = new URLSearchParams()
    params.append('offset', String(offset))
    params.append('limit', String(limit))

    if (dataset) params.append('dataset', dataset)
    if (date_from) params.append('date_from', date_from)
    if (date_to) params.append('date_to', date_to)
    if (facility_id) params.append('facility_id', facility_id)

    const separator = url.includes('?') ? '&' : '?'
    return `${url}${separator}${params.toString()}`
  }

  return useInfiniteQuery({
    ...options,
    queryKey: [key, { dataset, date_from, date_to, facility_id }],
    queryFn: ({ pageParam = 0 }) => {
      return apiFetch({ url: buildUrl(pageParam), method: 'GET' })
    },
    getNextPageParam: lastPage => {
      const { offset, limit, total_count } = lastPage
      const nextOffset = offset + limit
      return nextOffset < total_count ? nextOffset : undefined
    },
    initialPageParam: 0,
    staleTime: STALE_TIME_5M,
  })
}

export const useApiMutation = (
  { url, method, keys, ...options }:
  { url: string, method: Methods, keys: string[] }
) => {
  const ALLOWED_METHODS = ['POST', 'PUT', 'PATCH']
  if (!ALLOWED_METHODS.includes(method)) throw new TypeError(`Error: useApiMutation does not support method: ${method}`)

  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: payload => apiFetch({ url, payload, method,...options }),
    onSuccess: () => {
      keys.forEach(key => {
        queryClient.invalidateQueries({ queryKey: [key] })
      })
    }
  })
}


export const useApiDelete = (
  { url, keys }
  : { url: string, keys: string[] }
) => {
  const queryClient = useQueryClient()

  return useMutation({
    mutationFn: payload => apiFetch({ url, payload, method: 'DELETE' }),
    onSuccess: () => {
      keys.forEach(key => {
        queryClient.invalidateQueries({ queryKey: [key] })
      })
    }
  })
}