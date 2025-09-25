import "jsr:@supabase/functions-js/edge-runtime.d.ts"
import { createClient } from 'https://esm.sh/@supabase/supabase-js@2'

const corsHeaders = {
  'Access-Control-Allow-Origin': '*',
  'Access-Control-Allow-Headers': 'authorization, x-client-info, apikey, content-type',
  'Access-Control-Allow-Methods': 'POST, GET, OPTIONS, PUT, DELETE',
}

// We'll make this interface dynamic, based on the categories in the database
interface ProcessingResult {
  protectionist: -1 | 0 | 1
  progressive: -1 | 0 | 1
  // Other categories will be added dynamically
  [key: string]: 'low' | 'medium' | 'high' | -1 | 0 | 1 | number
}

async function processWithLLM(
  qaText: string, 
  categories: string[],
  numericalScores: Record<string, number>
): Promise<{ summary: string; scores: ProcessingResult }> {
  // Format the categories as a readable list for the prompt
  const categoriesList = categories.map(cat => 
    cat.split('_').map(word => word.charAt(0).toUpperCase() + word.slice(1)).join('-')
  ).join('\n');
  
  const prompt = `Based on this set of answers and responses below, create a 
Summary report of the regulatory mindset of the respondent to the questions.
It should estimate a value of low, medium, and high for each of the following tech
regulation mindset characteristics:

${categoriesList}

In the summary section, provide a rating of  '-1: Negative', '0: Neutral', '+1: Positive' for  
the respondent's mindset on being 'Protectionist' and a rating of '-1: Negative', '0: Neutral', '+1 Positive' for 'Progressive'.

The report should be written in a helpful tone directly to the respondent, 
but don't start with a name. Begin with 'Thank you for taking the time 
to provide your insights on technology regulation.'

Additionally, provide a JSON object at the end of your response, enclosed in a Markdown code block (\`\`\`json...\`\`\`). This JSON object should contain the following keys:

1. All category keys with their corresponding values (low, medium, high):
${categories.map(cat => `"${cat}": "low|medium|high"`).join(',\n')}

2. The two special assessment keys:
"protectionist": -1|0|1,
"progressive": -1|0|1

The JSON should also include the numerical scores I've calculated, under the same keys but with "_score" appended:
${Object.entries(numericalScores).map(([cat, score]) => 
  `"${cat}_score": ${score.toFixed(2)}`
).join(',\n')}

These are the questions and answers:

${qaText}`

  const response = await fetch('https://api.openai.com/v1/chat/completions', {
    method: 'POST',
    headers: {
      'Authorization': `Bearer ${Deno.env.get('OPENAI_API_KEY')}`,
      'Content-Type': 'application/json',
    },
    body: JSON.stringify({
      model: 'gpt-4',
      messages: [
        { role: 'user', content: prompt }
      ],
      max_tokens: 1500,
      temperature: 0.7,
    }),
  })

  const result = await response.json()
  const content = result.choices[0].message.content

  const jsonMatch = content.match(/```json\s*([\s\S]*?)\s*```/)
  if (!jsonMatch) {
    throw new Error('No JSON found in LLM response')
  }

  const scores = JSON.parse(jsonMatch[1])
  const summary = content.replace(/```json[\s\S]*?```/, '').trim()

  // Merge numerical scores into the results
  Object.entries(numericalScores).forEach(([category, score]) => {
    scores[`${category}_score`] = score
  })

  return { summary, scores }
}

// Function to calculate numerical scores per category
function calculateCategoryScores(
  questions: any[], 
  responses: Record<string, { rating: number, explanation?: string }>
): Record<string, number> {
  // Group questions by category
  const categoriesMap: Record<string, { score: number, count: number, totalWeight: number }> = {}
  
  questions.forEach(question => {
    const response = responses[question.id.toString()]
    if (!response || response.rating === undefined) return
    
    const category = question.category.toLowerCase().replace(/\s+/g, '_')
    const rating = response.rating
    const weight = question.weight || 1 // Default weight to 1
    const isForward = Number(question.forward) > 0;

    // Adjust rating if not forward (reverse the scale)
    const adjustedRating = isForward ? rating : (6 - rating)
    const weightedRating = adjustedRating * weight
    
    if (!categoriesMap[category]) {
      categoriesMap[category] = { score: 0, count: 0, totalWeight: 0 }
    }
    
    categoriesMap[category].score += weightedRating
    categoriesMap[category].count += 1
    categoriesMap[category].totalWeight += weight
  })
  
  // Calculate average scores per category
  const categoryScores: Record<string, number> = {}
  Object.entries(categoriesMap).forEach(([category, data]) => {
    if (data.totalWeight > 0) {
      categoryScores[category] = data.score / data.totalWeight
    } else {
      categoryScores[category] = 0
    }
  })
  
  return categoryScores
}

Deno.serve(async (req) => {
  if (req.method === 'OPTIONS') {
    return new Response('ok', { headers: corsHeaders })
  }

  try {
    const supabaseClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? ''
    )

    // Create queue client
    const queueClient = createClient(
      Deno.env.get('SUPABASE_URL') ?? '',
      Deno.env.get('SUPABASE_SERVICE_ROLE_KEY') ?? '',
      {
        db: { schema: 'pgmq_public' }
      }
    )

    // Get questions once at the beginning
    const { data: questions, error: questionsError } = await supabaseClient
      .from('questions')
      .select('*')
      .order('id', { ascending: true })

    if (questionsError) {
      throw new Error(`Failed to fetch questions: ${questionsError.message}`)
    }

    // Extract unique categories from questions
    const uniqueCategories = Array.from(
      new Set(questions?.map(q => q.category?.toLowerCase().replace(/\s+/g, '_')).filter(Boolean))
    ) as string[]

    console.log(`Found ${uniqueCategories.length} unique categories: ${uniqueCategories.join(', ')}`)

    const results = []
    const maxMessages = 5 // Process up to 5 messages per run

    // Process messages from the queue
    for (let i = 0; i < maxMessages; i++) {
      // Define message outside the try block so it's available in catch
      let message = null;
      
      try {
        // Read a message from the queue
        const { data: messages, error: readError } = await queueClient.rpc('read', {
          n: 1,
          queue_name: 'submissions',
          sleep_seconds: 30  // Increased visibility timeout to 30 seconds
        })

        if (readError) {
          console.error('Queue read error:', readError)
          break
        }

        if (!messages || messages.length === 0) {
          console.log('No messages in queue')
          break
        }

        message = messages[0]
        
        if (!message || !message.msg_id || !message.message) {
          console.error('Invalid message format:', message)
          // Skip this message and continue with the next one
          continue
        }
        
        const { submission_id, responses } = message.message
        
        if (!submission_id || !responses) {
          console.error('Message missing required fields:', message.message)
          // Delete invalid message
          await queueClient.rpc('delete', {
            queue_name: 'submissions',
            message_id: message.msg_id
          }).catch(err => console.error('Failed to delete invalid message:', err))
          continue
        }

        console.log(`Processing submission ${submission_id} from queue`)

        // Update submission status to processing
        const { error: updateStatusError } = await supabaseClient
          .from('submissions')
          .update({ status: 'processing' })
          .eq('id', submission_id)
          
        if (updateStatusError) {
          throw new Error(`Failed to update status: ${updateStatusError.message}`)
        }

        // Calculate numerical scores for each category
        const numericalScores = calculateCategoryScores(questions || [], responses)

        // Format Q&A text for LLM using the questions fetched once
        let qaText = ''
        questions?.forEach(question => {
          const response = responses[question.id.toString()]
          if (response) {
            qaText += `Q: ${question.question_text}\n`
            qaText += `Category: ${question.category}\n`
            qaText += `Rating: ${response.rating}/5\n`
            if (response.explanation) {
              qaText += `Explanation: ${response.explanation}\n`
            }
            qaText += '\n'
          }
        })

        // Process with LLM, passing categories and numerical scores
        const { summary, scores } = await processWithLLM(qaText, uniqueCategories, numericalScores)

        // Update submission with results
        const { error: updateError } = await supabaseClient
          .from('submissions')
          .update({
            status: 'processed',
            processed_at: new Date().toISOString(),
            summary: summary,
            scores: scores
          })
          .eq('id', submission_id)

        if (updateError) {
          throw new Error(`Failed to update submission ${submission_id}: ${updateError.message}`)
        }

        // Delete message from queue after successful processing
        const { error: deleteError } = await queueClient.rpc('delete', {
          queue_name: 'submissions',
          message_id: message.msg_id
        })
        
        if (deleteError) {
          console.error(`Failed to delete message ${message.msg_id}:`, deleteError)
          throw new Error(`Failed to delete message from queue: ${deleteError.message}`)
        }

        console.log(`Successfully processed and deleted message for submission ${submission_id}`)
        results.push({ submissionId: submission_id, status: 'success' })

      } catch (error) {
        console.error(`Error processing message:`, error)
        
        // Only try to update and delete if message is valid
        if (message && message.message && message.message.submission_id) {
          try {
            // Mark submission as failed 
            await supabaseClient
              .from('submissions')
              .update({
                status: 'failed',
                processed_at: new Date().toISOString(),
                error_message: error.message
              })
              .eq('id', message.message.submission_id)
              
            // Try to delete the failed message even if update fails
            if (message.msg_id) {
              const { error: deleteError } = await queueClient.rpc('delete', {
                queue_name: 'submissions',
                message_id: message.msg_id
              })
              
              if (deleteError) {
                console.error(`Failed to delete failed message ${message.msg_id}:`, deleteError)
              } else {
                console.log(`Deleted failed message for submission ${message.message.submission_id}`)
              }
            }
          } catch (innerError) {
            console.error('Error handling failed message:', innerError)
          }
        }

        results.push({ 
          submissionId: message?.message?.submission_id || 'unknown', 
          status: 'failed', 
          error: error.message 
        })
      }
    }

    return new Response(
      JSON.stringify({ 
        processed: results.length,
        results: results
      }),
      { headers: { ...corsHeaders, "Content-Type": "application/json" } }
    )

  } catch (error) {
    console.error('Error:', error)
    return new Response(
      JSON.stringify({ error: 'Internal server error', details: error.message }),
      { 
        status: 500,
        headers: { ...corsHeaders, "Content-Type": "application/json" }
      }
    )
  }
})