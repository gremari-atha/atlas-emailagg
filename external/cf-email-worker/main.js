/**
 * Cloudflare Email Routing Worker for Atlas Email Aggregator
 *
 * This Worker intercepts incoming emails routed via Cloudflare Email Routing,
 * parses the MIME payload using 'postal-mime', and forwards the structured
 * email data to the Atlas Email Aggregator webhook endpoint.
 *
 * Deployment requirements:
 * 1. Install postal-mime: `npm install postal-mime`
 * 2. Deploy this worker to Cloudflare.
 * 3. Configure Email Routing in Cloudflare Dashboard to forward emails to this Worker.
 */

import PostalMime from "postal-mime";

// CONFIGURATION: Set your webhook endpoint and validation token.
// The validation token must match the one you input in the Atlas Dashboard.
const BASE_URL = "https://emailagg.atlasroot.net/webhooks/cloudflare";
const VALIDATION_TOKEN = "YOUR_SECURE_VALIDATION_TOKEN_HERE";

export default {
  async email(message, env, ctx) {
    try {
      const subject = message.headers.get("subject") || "(No Subject)";
      const fromEmail = message.from;
      const toEmail = message.to;

      // 1. Send metadata pre-check request to Atlas Email Aggregator
      const preCheckResponse = await fetch(`${BASE_URL}/pre-check`, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Atlas-Webhook-Token": VALIDATION_TOKEN
        },
        body: JSON.stringify({
          to: toEmail,
          from: fromEmail,
          subject: subject
        })
      });

      if (!preCheckResponse.ok) {
        console.error(`Pre-check request failed: ${preCheckResponse.status}`);
        message.setReject(`Aggregator pre-check failed: ${preCheckResponse.status}`);
        return;
      }

      const preCheckResult = await preCheckResponse.json();
      if (!preCheckResult.match) {
        console.log(`Email discarded by pre-check (Subject: "${subject}" does not match active rules).`);
        return; // Discard email immediately (saves bandwidth and CPU)
      }

      console.log(`Pre-check matched for subject "${subject}". Parsing full email body...`);

      // 2. Read raw MIME stream from message
      const rawEmail = await readReadableStream(message.raw);

      // 3. Parse the MIME email using postal-mime
      const parser = new PostalMime();
      const parsedEmail = await parser.parse(rawEmail);

      // Extract body text or fall back to HTML content
      const bodyText = parsedEmail.text || "";
      const bodyHtml = parsedEmail.html || "";

      // Extract unique Message ID from headers or envelope
      const messageId = parsedEmail.messageId || `cf-${Date.now()}-${Math.random().toString(36).substr(2, 9)}`;
      
      // Parse date or default to now
      const emailDate = parsedEmail.date || new Date().toISOString();

      // 4. Construct full payload
      const payload = {
        message_id: messageId,
        to: toEmail,
        from: fromEmail,
        subject: subject,
        body_text: bodyText,
        body_html: bodyHtml,
        date: emailDate
      };

      // 5. Send POST webhook request with full payload
      const response = await fetch(BASE_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Atlas-Webhook-Token": VALIDATION_TOKEN
        },
        body: JSON.stringify(payload)
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`Aggregator full webhook failed: ${response.status} - ${errorText}`);
        message.setReject(`Failed to forward full email: ${response.status}`);
        return;
      }

      console.log(`Successfully forwarded email ${messageId} to aggregator.`);

    } catch (err) {
      console.error("Error processing incoming email in worker:", err);
      message.setReject(`Internal worker error: ${err.message}`);
    }
  }
};

/**
 * Helper to convert a ReadableStream into an ArrayBuffer/string for postal-mime
 */
async function readReadableStream(stream) {
  const reader = stream.getReader();
  const chunks = [];
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }
  
  // Combine chunks into a single Uint8Array
  const totalLength = chunks.reduce((acc, val) => acc + val.length, 0);
  const result = new Uint8Array(totalLength);
  let offset = 0;
  for (const chunk of chunks) {
    result.set(chunk, offset);
    offset += chunk.length;
  }
  return result.buffer;
}
