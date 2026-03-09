/**
 * Sentiment SDK — Drop-in client for any project
 * ================================================
 * Copy this file into your project and use:
 *
 *   import { Sentiment } from './sentiment-sdk'
 *
 *   const client = new Sentiment({
 *     url: 'https://your-app.com',
 *     key: 'sk-sent-...',
 *   })
 *
 *   const result = await client.analyze('Bitcoin is pumping!', 'twitter')
 *   console.log(result.label)  // "POSITIVE"
 */

interface SentimentConfig {
  url: string
  key: string
}

interface AnalyzeResult {
  label: 'POSITIVE' | 'NEUTRAL' | 'NEGATIVE'
  score: number
  source: string
  meta: {
    engineVersion: string
    confidence: string
    confidenceScore: number
    processingTimeMs: number
    cached: boolean
    detected: {
      positiveWords: string[]
      negativeWords: string[]
      neutralWords: string[]
    }
  }
}

interface NormalizeResult {
  original: string
  cleaned: string
  tokens: string[]
  lang: 'en' | 'ru' | 'ua' | 'unknown'
  charCount: number
  wordCount: number
}

interface BatchResult {
  results: Array<{
    id: string
    result: AnalyzeResult | null
    error: string | null
  }>
  meta: {
    totalItems: number
    successCount: number
    errorCount: number
    processingTimeMs: number
  }
}

export class Sentiment {
  private url: string
  private key: string

  constructor(config: SentimentConfig) {
    this.url = config.url.replace(/\/$/, '')
    this.key = config.key
  }

  private async request<T>(method: string, path: string, body?: any): Promise<T> {
    const res = await fetch(`${this.url}/api/v1/sentiment${path}`, {
      method,
      headers: {
        'Content-Type': 'application/json',
        'X-API-Key': this.key,
      },
      body: body ? JSON.stringify(body) : undefined,
    })

    const json = await res.json()
    if (!json.ok) throw new Error(json.message || json.error || 'API error')
    return json.data
  }

  /** Analyze a single text */
  async analyze(text: string, source?: string): Promise<AnalyzeResult> {
    return this.request('POST', '/analyze', { text, source })
  }

  /** Analyze multiple texts at once (up to 100) */
  async batch(items: Array<{ id: string; text: string; source?: string }>, source?: string): Promise<BatchResult> {
    return this.request('POST', '/batch', { items, source })
  }

  /** Normalize text: cleanup, tokenize, detect language */
  async normalize(text: string): Promise<NormalizeResult> {
    return this.request('POST', '/normalize', { text })
  }

  /** Check engine health */
  async health(): Promise<{ status: string; engineVersion: string }> {
    return this.request('GET', '/health')
  }
}
