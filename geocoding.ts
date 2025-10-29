import axios, { AxiosError } from "axios";
import {
  getProviderTokenEntries,
  recordTokenUsage,
  TokenProvider,
  TokenUsageInfo,
} from "./tokenManager";

export interface Coordinates {
  latitude: number;
  longitude: number;
}

interface ProviderState {
  type: GeocodeProviderInput["type"];
  name: string;
  cooldownMs: number;
  availableAt: number;
  tokenId?: string;
  disabled?: boolean;
  networkErrorCount?: number;
  lastErrorTimestamp?: number;
  request: (address: string) => Promise<Coordinates | null>;
}

export interface GeocoderLogger {
  info?(msg: string): void;
  warn?(msg: string): void;
  error?(msg: string, err?: unknown): void;
}

export interface GeocodeProviderInput {
  type: "mapsco" | "locationiq" | "geoapify" | "opencage" | "nominatim";
  name?: string;
  apiKey?: string;
  baseUrl?: string;
  cooldownMs?: number;
  enabled?: boolean;
  tokenId?: string;
}

export interface CreateGeocoderOptions {
  logger?: GeocoderLogger;
  userAgent?: string;
  providers?: GeocodeProviderInput[];
  defaultCooldownMs?: number;
}

const DEFAULT_USER_AGENT =
  process.env.GEOCODER_USER_AGENT ?? "etl-geosaude-itajuba/1.0 (contact: data-team@localhost)";

const delay = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms));

function parseNumber(input: unknown): number | null {
  const value = typeof input === "number" ? input : Number(input);
  if (!Number.isFinite(value)) return null;
  return value;
}

function formatLastUsed(usage: TokenUsageInfo): string {
  if (!usage.lastUsedAt) return "nunca";
  try {
    return new Date(usage.lastUsedAt).toLocaleString();
  } catch {
    return usage.lastUsedAt;
  }
}

function buildDefaultProviders(logger?: GeocoderLogger): GeocodeProviderInput[] {
  const providers: GeocodeProviderInput[] = [];

  const appendTokens = (providerType: TokenProvider, fallbackCooldown: number) => {
    const entries = getProviderTokenEntries(providerType);
    entries.forEach((entry) => {
      providers.push({
        type: providerType,
        name: entry.label,
        apiKey: entry.token,
        cooldownMs: entry.cooldownMs ?? fallbackCooldown,
        tokenId: entry.id,
      });
      logger?.info?.(
        `[Tokens] ${entry.label} | usado hoje: ${entry.usage.usedToday ? "sim" : "não"} | última vez: ${formatLastUsed(entry.usage)}`,
      );
    });
  };

  appendTokens("locationiq", 600);
  appendTokens("geoapify", 250);
  appendTokens("opencage", 1100);

  if ((process.env.GEOCODER_ENABLE_NOMINATIM ?? "").toLowerCase() === "true") {
    providers.push({
      type: "nominatim",
      name: "OSM Nominatim",
      cooldownMs: 1500,
    });
  }

  providers.push({
    type: "mapsco",
    name: "MapsCo",
    cooldownMs: 1500,
  });

  return providers;
}

function createProviderState(
  provider: GeocodeProviderInput,
  userAgent: string,
  defaultCooldownMs: number,
): ProviderState | null {
  if (provider.enabled === false) {
    return null;
  }

  const name = provider.name ?? provider.type;
  const cooldownMs = Math.max(0, provider.cooldownMs ?? defaultCooldownMs);

  switch (provider.type) {
    case "mapsco": {
      const baseUrl = provider.baseUrl ?? "https://geocode.maps.co";
      return {
        type: "mapsco",
        name,
        cooldownMs,
        availableAt: 0,
        disabled: false,
        networkErrorCount: 0,
        async request(address: string) {
          const url = `${baseUrl.replace(/\/+$/, "")}/search`;
          try {
            const { data } = await axios.get(url, {
              timeout: 10000,
              params: { q: address },
              headers: {
                "User-Agent": userAgent,
                Accept: "application/json",
              },
            });
            if (Array.isArray(data) && data.length > 0) {
              const lat = parseNumber(data[0]?.lat);
              const lon = parseNumber(data[0]?.lon);
              if (lat !== null && lon !== null) {
                return { latitude: lat, longitude: lon };
              }
            }
            return null;
          } catch (err) {
            if (isNotFoundError(err)) {
              return null;
            }
            throw err;
          }
        },
      };
    }
    case "locationiq": {
      if (!provider.apiKey) return null;
      const baseUrl = provider.baseUrl ?? "https://us1.locationiq.com/v1/search";
      return {
        type: "locationiq",
        name,
        cooldownMs,
        availableAt: 0,
        tokenId: provider.tokenId,
        disabled: false,
        networkErrorCount: 0,
        async request(address: string) {
          if (provider.tokenId) {
            recordTokenUsage(provider.tokenId);
          }
          try {
            const { data } = await axios.get(baseUrl, {
              timeout: 10000,
              params: {
                key: provider.apiKey,
                q: address,
                format: "json",
                normalizecity: 1,
              },
              headers: { "User-Agent": userAgent },
            });
            if (Array.isArray(data) && data.length > 0) {
              const lat = parseNumber(data[0]?.lat);
              const lon = parseNumber(data[0]?.lon);
              if (lat !== null && lon !== null) {
                return { latitude: lat, longitude: lon };
              }
            }
            return null;
          } catch (err) {
            if (isNotFoundError(err)) {
              return null;
            }
            throw err;
          }
        },
      };
    }
    case "geoapify": {
      if (!provider.apiKey) return null;
      const baseUrl = provider.baseUrl ?? "https://api.geoapify.com/v1/geocode/search";
      return {
        type: "geoapify",
        name,
        cooldownMs,
        availableAt: 0,
        tokenId: provider.tokenId,
        disabled: false,
        networkErrorCount: 0,
        async request(address: string) {
          if (provider.tokenId) {
            recordTokenUsage(provider.tokenId);
          }
          try {
            const { data } = await axios.get(baseUrl, {
              timeout: 10000,
              params: {
                text: address,
                lang: "pt",
                limit: 1,
                apiKey: provider.apiKey,
              },
              headers: { "User-Agent": userAgent },
            });
            const feature = data?.features?.[0];
            const lat = parseNumber(feature?.properties?.lat ?? feature?.geometry?.coordinates?.[1]);
            const lon = parseNumber(feature?.properties?.lon ?? feature?.geometry?.coordinates?.[0]);
            if (lat !== null && lon !== null) {
              return { latitude: lat, longitude: lon };
            }
            return null;
          } catch (err) {
            if (isNotFoundError(err)) {
              return null;
            }
            throw err;
          }
        },
      };
    }
    case "opencage": {
      if (!provider.apiKey) return null;
      const baseUrl = provider.baseUrl ?? "https://api.opencagedata.com/geocode/v1/json";
      return {
        type: "opencage",
        name,
        cooldownMs,
        availableAt: 0,
        tokenId: provider.tokenId,
        disabled: false,
        networkErrorCount: 0,
        async request(address: string) {
          if (provider.tokenId) {
            recordTokenUsage(provider.tokenId);
          }
          try {
            const { data } = await axios.get(baseUrl, {
              timeout: 10000,
              params: {
                q: address,
                key: provider.apiKey,
                language: "pt",
                limit: 1,
                countrycode: "br",
              },
              headers: { "User-Agent": userAgent },
            });
            const result = Array.isArray(data?.results) ? data.results[0] : null;
            const lat = parseNumber(result?.geometry?.lat);
            const lon = parseNumber(result?.geometry?.lng);
            if (lat !== null && lon !== null) {
              return { latitude: lat, longitude: lon };
            }
            return null;
          } catch (err) {
            if (isNotFoundError(err)) {
              return null;
            }
            throw err;
          }
        },
      };
    }
    case "nominatim": {
      const baseUrl = provider.baseUrl ?? "https://nominatim.openstreetmap.org/search";
      return {
        type: "nominatim",
        name,
        cooldownMs,
        availableAt: 0,
        disabled: false,
        networkErrorCount: 0,
        async request(address: string) {
          try {
            const { data } = await axios.get(baseUrl, {
              timeout: 10000,
              params: {
                format: "json",
                addressdetails: 0,
                limit: 1,
                q: address,
              },
              headers: {
                "User-Agent": userAgent,
                "Accept-Language": "pt-BR,en",
              },
            });
            if (Array.isArray(data) && data.length > 0) {
              const lat = parseNumber(data[0]?.lat);
              const lon = parseNumber(data[0]?.lon);
              if (lat !== null && lon !== null) {
                return { latitude: lat, longitude: lon };
              }
            }
            return null;
          } catch (err) {
            if (isNotFoundError(err)) {
              return null;
            }
            throw err;
          }
        },
      };
    }
    default:
      return null;
  }
}

function isRateLimitError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) return false;
  const status = error.response?.status;
  if (status === 429 || status === 402 || status === 403) return true;
  const message = String(error.message || "").toLowerCase();
  if (message.includes("rate") && message.includes("limit")) return true;
  return false;
}

function isNotFoundError(error: unknown): boolean {
  return axios.isAxiosError(error) && error.response?.status === 404;
}

function describeError(error: unknown): string {
  if (axios.isAxiosError(error)) {
    const status = error.response?.status;
    if (status) return `HTTP ${status}`;
    if (error.code) return error.code;
  }
  return (error as Error)?.message ?? String(error);
}

function isNetworkError(error: unknown): boolean {
  if (!axios.isAxiosError(error)) {
    if (error instanceof Error) {
      return /timeout/i.test(error.message) || /eai_again/i.test(error.message);
    }
    return false;
  }
  const code = error.code?.toUpperCase();
  if (!code) return false;
  const networkCodes = new Set([
    "EAI_AGAIN",
    "ENOTFOUND",
    "ECONNREFUSED",
    "ETIMEDOUT",
    "ESOCKETTIMEDOUT",
    "ECONNRESET",
    "EHOSTUNREACH",
    "ENETUNREACH",
  ]);
  return networkCodes.has(code);
}

export type GeocodeFn = (address: string) => Promise<Coordinates | null>;

export function createGeocoder(options: CreateGeocoderOptions = {}): GeocodeFn | null {
  const userAgent = options.userAgent ?? DEFAULT_USER_AGENT;
  const defaultCooldown = Math.max(0, options.defaultCooldownMs ?? 1000);
  const requestedProviders = options.providers ?? buildDefaultProviders(options.logger);

  const providers: ProviderState[] = [];
  for (const provider of requestedProviders) {
    const state = createProviderState(provider, userAgent, defaultCooldown);
    if (state) {
      providers.push(state);
    }
  }

  if (!providers.length) {
    options.logger?.warn?.("Nenhum provedor de geocodificação configurado. Endereços ficarão sem coordenadas.");
    return null;
  }

  return async (address: string) => {
    if (!address.trim()) return null;

    const logger = options.logger;
    const maxRounds = Math.max(1, providers.length);
    let lastError: unknown = null;

    for (let round = 0; round < maxRounds; round += 1) {
      const now = Date.now();
      let nextAvailable = Number.POSITIVE_INFINITY;
      let attempted = false;
      const activeProviders = providers.filter((p) => !p.disabled);
      if (!activeProviders.length) {
        logger?.warn?.("Todos os provedores de geocodificação foram desativados após falhas de rede.");
        break;
      }

      for (const provider of activeProviders) {
        const availableIn = provider.availableAt - now;
        if (availableIn > 0) {
          nextAvailable = Math.min(nextAvailable, availableIn);
          continue;
        }

        attempted = true;
        try {
          const result = await provider.request(address);
          provider.availableAt = Date.now() + provider.cooldownMs;
          if (result) {
            return result;
          }
        } catch (err) {
          lastError = err;
          provider.availableAt = Date.now() + provider.cooldownMs;
           provider.lastErrorTimestamp = Date.now();
          if (isRateLimitError(err)) {
            logger?.warn?.(
              `${provider.name} atingiu limite de taxa. Aguardando ${provider.cooldownMs}ms antes de reutilizar.`,
            );
            continue;
          }
          if (axios.isAxiosError(err)) {
            const status = err.response?.status;
            if (status && status >= 500) {
              logger?.warn?.(`${provider.name} respondeu ${describeError(err)}. Tentando próximo provedor.`);
              continue;
            }
          }
          if (isNetworkError(err)) {
            provider.networkErrorCount = (provider.networkErrorCount ?? 0) + 1;
            const retryDelay = Math.min(30_000, provider.cooldownMs * (1 + provider.networkErrorCount));
            provider.availableAt = Date.now() + retryDelay;
            if ((provider.networkErrorCount ?? 0) >= 3) {
              provider.disabled = true;
              logger?.warn?.(
                `${provider.name} desativado temporariamente após ${provider.networkErrorCount} falhas de rede consecutivas (${describeError(err)}).`,
              );
            } else {
              logger?.warn?.(
                `${provider.name} indisponível por rede (${describeError(err)}). Nova tentativa após ${retryDelay}ms.`,
              );
            }
            continue;
          }
          logger?.error?.(`${provider.name} falhou ao geocodificar: ${describeError(err)}`, err);
        }
      }

      if (attempted) {
        break;
      }

      if (nextAvailable !== Number.POSITIVE_INFINITY) {
        const waitMs = Math.min(nextAvailable, 10_000);
        if (waitMs > 0) {
          logger?.warn?.(`Todos os provedores em cooldown. Aguardando ${Math.round(waitMs)}ms.`);
          await delay(waitMs);
        }
      } else {
        break;
      }
    }

    if (lastError && !isRateLimitError(lastError)) {
      logger?.warn?.(`Geocodificação falhou para "${address}": ${describeError(lastError)}`);
    }

    return null;
  };
}
