import * as fs from "fs";
import * as path from "path";

export type TokenProvider = "locationiq" | "geoapify" | "opencage";

interface TokenConfigEntry {
  id: string;
  token: string;
  label: string;
  cooldownMs?: number;
}

interface TokenUsageStored {
  lastUsedAt: string;
}

type UsageMap = Record<string, TokenUsageStored>;

export interface TokenUsageInfo {
  lastUsedAt: string | null;
  usedToday: boolean;
}

export interface TokenEntry extends TokenConfigEntry {
  usage: TokenUsageInfo;
}

const TOKEN_CONFIG: Record<TokenProvider, TokenConfigEntry[]> = {
  locationiq: [
    {
      id: "locationiq:1",
      token: "pk.6173695384a78ce3448b567e320b2596",
      label: "LocationIQ #1",
      cooldownMs: 600,
    },
  ],
  geoapify: [
    {
      id: "geoapify:1",
      token: "1fac6ac86d1748a3a35f920906515bc5",
      label: "Geoapify #1",
      cooldownMs: 250,
    },
    {
      id: "geoapify:2",
      token: "1bbcb90d27d848d28968fe5ae417bf96",
      label: "Geoapify #2",
      cooldownMs: 250,
    },
  ],
  opencage: [
    {
      id: "opencage:1",
      token: "f7ec23df740d4697b214329872a85908",
      label: "OpenCage #1",
      cooldownMs: 1100,
    },
    {
      id: "opencage:2",
      token: "2458aa115c0d467da1971b60827ec693",
      label: "OpenCage #2",
      cooldownMs: 1100,
    },
    {
      id: "opencage:3",
      token: "ec09d0eecb8f44e7831e072556f768bd",
      label: "OpenCage #3",
      cooldownMs: 1100,
    },
  ],
};

const USAGE_FILE = path.resolve(process.cwd(), "token-usage.json");

export function hasAnyTokens(): boolean {
  return Object.values(TOKEN_CONFIG).some((entries) => entries.length > 0);
}

let usageCache: UsageMap | null = null;
const lastPersistedAt: Record<string, number> = {};
let exitHookRegistered = false;

function ensureUsageLoaded(): void {
  if (usageCache !== null) return;
  try {
    const raw = fs.readFileSync(USAGE_FILE, "utf-8");
    const parsed = JSON.parse(raw) as UsageMap;
    usageCache = parsed && typeof parsed === "object" ? parsed : {};
  } catch {
    usageCache = {};
  }
}

function saveUsage(): void {
  if (usageCache === null) return;
  try {
    const payload = JSON.stringify(usageCache, null, 2);
    fs.writeFileSync(USAGE_FILE, payload, "utf-8");
  } catch {
    // ignore persistence errors; availability info will be best-effort
  }
}

function ensureExitHook(): void {
  if (exitHookRegistered) return;
  exitHookRegistered = true;
  process.once("exit", () => {
    try {
      saveUsage();
    } catch {
      // ignore persistence errors on shutdown
    }
  });
}

function sameDay(a: Date, b: Date): boolean {
  return a.getFullYear() === b.getFullYear() && a.getMonth() === b.getMonth() && a.getDate() === b.getDate();
}

function buildUsageInfo(entry?: TokenUsageStored): TokenUsageInfo {
  if (!entry?.lastUsedAt) {
    return { lastUsedAt: null, usedToday: false };
  }
  const lastUsedDate = new Date(entry.lastUsedAt);
  const now = new Date();
  return {
    lastUsedAt: entry.lastUsedAt,
    usedToday: sameDay(lastUsedDate, now),
  };
}

export function getProviderTokenEntries(provider: TokenProvider): TokenEntry[] {
  ensureUsageLoaded();
  const configEntries = TOKEN_CONFIG[provider] ?? [];
  return configEntries.map((entry) => ({
    ...entry,
    usage: buildUsageInfo(usageCache?.[entry.id]),
  }));
}

export function recordTokenUsage(tokenId: string): TokenUsageInfo {
  ensureUsageLoaded();
  ensureExitHook();

  const now = new Date();
  const nowIso = now.toISOString();
  if (usageCache) {
    usageCache[tokenId] = { lastUsedAt: nowIso };
  }

  const lastPersisted = lastPersistedAt[tokenId] ?? 0;
  if (!lastPersisted || now.getTime() - lastPersisted >= 60_000) {
    lastPersistedAt[tokenId] = now.getTime();
    saveUsage();
  }

  return {
    lastUsedAt: nowIso,
    usedToday: true,
  };
}
