import { serve } from 'https://deno.land/std@0.168.0/http/server.ts'
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const KAGGLE_API_URL = 'https://www.kaggle.com/api/v1/datasets/erlichsefi/israeli-supermarkets-2024'
const SUPABASE_URL = Deno.env.get('SUPABASE_URL') ?? ''
const SUPABASE_ANON_KEY = Deno.env.get('SUPABASE_ANON_KEY') ?? ''
const KAGGLE_USERNAME = Deno.env.get('KAGGLE_USERNAME') ?? ''
const KAGGLE_KEY = Deno.env.get('KAGGLE_KEY') ?? ''

interface KaggleCheckResult {
  timestamp: string
  is_updated: boolean
  last_update: string | null
  error_message?: string
}

serve(async () => {
  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    // יצירת הרשאות Kaggle API
    const headers = new Headers()
    headers.append('Authorization', `Basic ${btoa(`${KAGGLE_USERNAME}:${KAGGLE_KEY}`)}`)
    
    // בקשה למידע על המאגר
    const response = await fetch(KAGGLE_API_URL, {
      headers: headers
    })
    
    if (!response.ok) {
      throw new Error(`HTTP error! status: ${response.status}`)
    }
    
    const data = await response.json()
    const lastUpdateTime = new Date(data.lastUpdated)
    const now = new Date()
    const hoursDiff = (now.getTime() - lastUpdateTime.getTime()) / (1000 * 60 * 60)
    
    const result: KaggleCheckResult = {
      timestamp: now.toISOString(),
      is_updated: hoursDiff <= 24,
      last_update: lastUpdateTime.toISOString()
    }
    
    // שמירת התוצאות במסד הנתונים
    const { error } = await supabase
      .from('kaggle_updates')
      .insert(result)
    
    if (error) throw error
    
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
      status: 200,
    })
    
  } catch (error) {
    const result: KaggleCheckResult = {
      timestamp: new Date().toISOString(),
      is_updated: false,
      last_update: null,
      error_message: error.message
    }
    
    // שמירת השגיאה במסד הנתונים
    const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
    await supabase.from('kaggle_updates').insert(result)
    
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
      status: 500,
    })
  }
}) 