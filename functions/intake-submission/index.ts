import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, PUT, DELETE',
}

Deno.serve(async (req) => {
  // Handle CORS preflight requests
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    )

    // Create a separate client for the queue with the pgmq_public schema
    const queueClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '',
      {
        db: { schema: 'pgmq_public' }
      }
    )

    const { responses } = await req.json()

    if (!responses || typeof responses !== 'object') {
      return new Response(
        JSON.stringify({ error: 'Invalid responses data' }),
        {
          status: 400,
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        }
      )
    }

    // Insert submission with pending status
    const { data: submission, error: submissionError } = await supabaseClient
      .from('submissions')
      .insert({
        status: 'pending',
        submitted_at: new Date().toISOString(),
        responses: responses
      })
      .select('id')
      .single()

    if (submissionError) {
      console.error('Submission error:', submissionError)
      return new Response(
        JSON.stringify({ error: 'Failed to save submission' }),
        {
          status: 500,
          headers: { ...corsHeaders, "Content-Type": "application/json" }
        }
      )
    }

    try {
      const { data, error: queueError } = await queueClient.rpc('send', {
        queue_name: 'submissions',
        message: {
          submission_id: submission.id,
          responses: responses,
          submitted_at: new Date().toISOString()
        }
      })

      if (queueError) {
        console.error('Queue error:', queueError)
      } else {
        console.log('Message queued:', data)
      }
    } catch (queueError) {
      console.error('Failed to add to queue:', queueError)
    }

    return new Response(
      JSON.stringify({
        success: true,
        submissionId: submission.id,
        message: 'Survey submitted successfully'
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )

  } catch (error) {
    console.error('Error:', error)
    return new Response(
      JSON.stringify({ error: 'Internal server error' }),
      {
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      }
    )
  }
})