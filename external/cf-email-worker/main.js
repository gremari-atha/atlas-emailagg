/**
 * Cloudflare Email Routing Worker for Atlas Email Aggregator (Zero-Dependency)
 *
 * This Worker intercepts incoming emails routed via Cloudflare Email Routing,
 * performs an authorization metadata precheck, and forwards the raw MIME EML stream
 * directly as binary body to the Atlas Email Aggregator webhook endpoint.
 *
 * Deployment requirements:
 * 1. Deploy this worker to Cloudflare (directly copy-paste this code into the browser editor).
 * 2. Configure Email Routing in Cloudflare Dashboard to forward emails to this Worker.
 */

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

      console.log(`Pre-check matched for subject "${subject}". Forwarding raw MIME stream...`);

      // 2. Forward the raw MIME email stream directly as binary body
      const response = await fetch(BASE_URL, {
        method: "POST",
        headers: {
          "Content-Type": "application/octet-stream",
          "X-Atlas-Webhook-Token": VALIDATION_TOKEN,
          "X-Atlas-Webhook-To": toEmail,
          "X-Atlas-Webhook-From": fromEmail,
          "X-Atlas-Webhook-Subject": subject
        },
        body: message.raw
      });

      if (!response.ok) {
        const errorText = await response.text();
        console.error(`Aggregator full webhook failed: ${response.status} - ${errorText}`);
        message.setReject(`Failed to forward raw email: ${response.status}`);
        return;
      }

      console.log(`Successfully forwarded raw email stream to aggregator.`);

    } catch (err) {
      console.error("Error processing incoming email in worker:", err);
      message.setReject(`Internal worker error: ${err.message}`);
    }
  }
};
