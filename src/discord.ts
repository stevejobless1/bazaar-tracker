import axios from 'axios';

const WEBHOOK_URL = process.env.DISCORD_WEBHOOK_URL || 'https://discord.com/api/webhooks/1308684170415050782/4QyQ9Wlp283FeVCpBbs-d8AnDXbTGI5_0tjs1wlrKViPpGNBPnEJhSmgOppOa8-tm9pX';

// Rate limit: max 1 message per 30 seconds per type to avoid Discord rate limits
const lastSent = new Map<string, number>();
const RATE_LIMIT_MS = 30_000;

type Severity = 'error' | 'warning' | 'info' | 'success';

const COLORS: Record<Severity, number> = {
  error:   0xff0040,  // Red
  warning: 0xffaa00,  // Amber
  info:    0x00e5ff,  // Cyan
  success: 0x3fb950,  // Green
};

const EMOJIS: Record<Severity, string> = {
  error:   '🔴',
  warning: '🟡',
  info:    '🔵',
  success: '🟢',
};

interface DiscordNotifyOptions {
  title: string;
  description: string;
  severity: Severity;
  fields?: { name: string; value: string; inline?: boolean }[];
  service?: string;
  mention?: boolean; // @everyone
}

export async function notifyDiscord(opts: DiscordNotifyOptions): Promise<void> {
  const { title, description, severity, fields = [], service = 'unknown', mention = false } = opts;

  // Rate limit by title to avoid spam
  const rateKey = `${severity}:${title}`;
  const now = Date.now();
  const last = lastSent.get(rateKey);
  if (last && now - last < RATE_LIMIT_MS) {
    return; // Skip — too soon
  }
  lastSent.set(rateKey, now);

  const embed = {
    title: `${EMOJIS[severity]} ${title}`,
    description,
    color: COLORS[severity],
    fields: [
      { name: 'Service', value: service, inline: true },
      { name: 'Timestamp', value: new Date().toISOString(), inline: true },
      ...fields,
    ],
    footer: {
      text: 'Bazaar Tracker Monitor',
    },
    timestamp: new Date().toISOString(),
  };

  const payload: any = {
    embeds: [embed],
  };

  // Only @everyone for errors
  if (mention || severity === 'error') {
    payload.content = '@everyone';
  }

  try {
    await axios.post(WEBHOOK_URL, payload, {
      headers: { 'Content-Type': 'application/json' },
      timeout: 5000,
    });
  } catch (err) {
    // Don't let webhook failures crash the service
    console.error('[Discord] Failed to send webhook:', (err as Error).message);
  }
}

// Convenience helpers
export const notifyError = (service: string, title: string, description: string, fields?: { name: string; value: string; inline?: boolean }[]) =>
  notifyDiscord({ title, description, severity: 'error', service, fields, mention: true });

export const notifyWarning = (service: string, title: string, description: string, fields?: { name: string; value: string; inline?: boolean }[]) =>
  notifyDiscord({ title, description, severity: 'warning', service, fields });

export const notifyInfo = (service: string, title: string, description: string, fields?: { name: string; value: string; inline?: boolean }[]) =>
  notifyDiscord({ title, description, severity: 'info', service, fields });

export const notifySuccess = (service: string, title: string, description: string, fields?: { name: string; value: string; inline?: boolean }[]) =>
  notifyDiscord({ title, description, severity: 'success', service, fields });

// Track consecutive failures for escalation
const failureCounters = new Map<string, number>();

export function trackFailure(service: string, context: string): number {
  const key = `${service}:${context}`;
  const count = (failureCounters.get(key) || 0) + 1;
  failureCounters.set(key, count);
  return count;
}

export function resetFailure(service: string, context: string): void {
  failureCounters.delete(`${service}:${context}`);
}

export function getFailureCount(service: string, context: string): number {
  return failureCounters.get(`${service}:${context}`) || 0;
}
