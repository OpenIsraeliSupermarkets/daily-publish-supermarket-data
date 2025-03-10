import { serve } from 'https://deno.land/std@0.168.0/http/server.ts'
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const ENDPOINT_URL = 'YOUR_API_URL/health'
const SUPABASE_URL = Deno.env.get('SUPABASE_URL') ?? ''
const SUPABASE_ANON_KEY = Deno.env.get('SUPABASE_ANON_KEY') ?? ''

interface HealthCheckResult {
  timestamp: string
  is_healthy: boolean
  response_time: number
  status_code: number
  error_message?: string
}

serve(async () => {
  try {
    const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
    
    // מדידת זמן התגובה
    const startTime = Date.now()
    const response = await fetch(ENDPOINT_URL)
    const responseTime = Date.now() - startTime
    
    const result: HealthCheckResult = {
      timestamp: new Date().toISOString(),
      is_healthy: response.ok,
      response_time: responseTime,
      status_code: response.status
    }
    
    if (!response.ok) {
      result.error_message = `HTTP error! status: ${response.status}`
    }
    
    // שמירת התוצאות במסד הנתונים
    const { error } = await supabase
      .from('health_checks')
      .insert(result)
    
    if (error) throw error
    
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
      status: 200,
    })
    
  } catch (error) {
    const result: HealthCheckResult = {
      timestamp: new Date().toISOString(),
      is_healthy: false,
      response_time: -1,
      status_code: 500,
      error_message: error.message
    }
    
    // שמירת השגיאה במסד הנתונים
    const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY)
    await supabase.from('health_checks').insert(result)
    
    return new Response(JSON.stringify(result), {
      headers: { 'Content-Type': 'application/json' },
      status: 500,
    })
  }
}) 