/* script.ts
 * ETL com logging detalhado + STREAMING para grandes arquivos.
 * - Logs coloridos no terminal
 * - Registro de falhas em arquivo (error_log.txt)
 * - Processamento e escrita em streaming (evita OutOfMemory)
 * - Progresso em tempo real por bytes e por registros
 */

import * as fs from "fs";
import * as fsp from "fs/promises";
import * as path from "path";
import * as readline from "readline";
import {
  normalizeString,
  fixEducationLevel,
  fixMaritalStatus,
  parseDate,
  extractNumber,
  extractStreet,
  extractComplement,
  ageFromBirthDate,
  uuidv4,
  sanitizeCEP,
  normalizeGender,
  stripDiacritics,
} from "./utils";
import { parseCustomJSON } from "./jsonParser";
import { createGeocoder, Coordinates, GeocodeFn } from "./geocoding";
import { hasAnyTokens } from "./tokenManager";

// ==============================
// 🎨 Cores simples (ANSI codes)
// ==============================
const color = {
  reset: "\x1b[0m",
  blue: "\x1b[34m",
  green: "\x1b[32m",
  yellow: "\x1b[33m",
  red: "\x1b[31m",
  gray: "\x1b[90m",
  bold: "\x1b[1m",
};

// ==============================
// 🪵 Sistema de Log
// ==============================
class Logger {
  private errorPath: string;
  private logPath: string;
  private startTime: number;

  constructor(outputDir: string) {
    try {
      fs.mkdirSync(outputDir, { recursive: true });
    } catch {
      // best effort; writes will fail if directory truly missing
    }
    this.logPath = path.join(outputDir, "etl.log");
    this.errorPath = path.join(outputDir, "error_log.txt");
    this.startTime = Date.now();
  }

  private appendLog(level: string, msg: string, details?: unknown) {
    const timestamp = new Date().toISOString();
    const base = `[${timestamp}] [${level}] ${msg}`;
    let detailBlock = "";
    if (details !== undefined) {
      if (details instanceof Error) {
        detailBlock = `\n${details.stack ?? details.message}`;
      } else if (typeof details === "string") {
        detailBlock = `\n${details}`;
      } else {
        try {
          detailBlock = `\n${JSON.stringify(details, null, 2)}`;
        } catch {
          detailBlock = `\n${String(details)}`;
        }
      }
    }
    try {
      fs.appendFileSync(this.logPath, `${base}${detailBlock}\n`, "utf-8");
    } catch {
      // ignore logging persistence errors
    }
  }

  info(msg: string) {
    console.log(`${color.blue}[INFO]${color.reset} ${msg}`);
    this.appendLog("INFO", msg);
  }

  progress(msg: string) {
    // Linha de progresso diferenciada
    console.log(`${color.gray}[PROGRESS]${color.reset} ${msg}`);
    this.appendLog("PROGRESS", msg);
  }

  success(msg: string) {
    console.log(`${color.green}[OK]${color.reset} ${msg}`);
    this.appendLog("SUCCESS", msg);
  }

  warn(msg: string) {
    console.warn(`${color.yellow}[WARN]${color.reset} ${msg}`);
    this.appendLog("WARN", msg);
  }

  error(msg: string, details?: any) {
    console.error(`${color.red}[ERROR]${color.reset} ${msg}`);
    const entry = `[${new Date().toISOString()}] ${msg}\n${
      details ? (typeof details === "string" ? details : (() => {
        try {
          return JSON.stringify(details, null, 2);
        } catch {
          return String(details);
        }
      })()) : ""
    }\n\n`;
    fs.appendFileSync(this.errorPath, entry, "utf-8");
    this.appendLog("ERROR", msg, details);
  }

  debug(msg: string) {
    console.log(`${color.gray}[DEBUG]${color.reset} ${msg}`);
    this.appendLog("DEBUG", msg);
  }

  done(total: number, success: number, failed: number) {
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(2);
    const msg = `🏁 Finalizado: ${success}/${total} registros processados com sucesso (${failed} falhas) em ${elapsed}s`;
    console.log(`${color.bold}${color.green}${msg}${color.reset}`);
    this.appendLog("DONE", msg, { total, success, failed, elapsedSeconds: elapsed });
  }
}

// ==============================
// 📋 Tipos e Interfaces
// ==============================

interface RawPatientData {
  nr_atendimento?: number;
  ds_paciente?: string;
  nm_paciente?: string;
  nr_anos?: number | string;
  dt_nascimento?: string;
  ds_genero?: string;
  ds_sexo?: string;
  ie_sexo?: string;
  estado_civil?: string;
  ie_grau_instrucao?: string;
  ds_proc_principal?: string;
  ds_endereco?: string;
  cd_cep?: string;
  ds_bairro?: string;
  ds_nivel_urgencia?: string;
  cd_cid_principal?: string;
  ds_convenio?: string;
  ds_setor_atendimento?: string;
  dt_entrada?: string;
  dt_alta?: string;
  ds_motivo_alta?: string;
}

interface CleanedAttendance {
  id: string;
  patient_id: string;
  convenio: string;
  nivel_urgencia: string;
  setor_atendimento: string;
  cid_principal?: string | null;
  proc_principal?: string | null;
  motivo_alta?: string | null;
  entrada: string;
  alta?: string | null;
}

interface CleanedPatient {
  id: string;
  nome: string;
  birth_date: string | null;
  age: number | null;
  gender: string;
  education_level: string;
  marital_status: string;
  age_description?: string | null;
  address: {
    street: string;
    number: string;
    complement: string;
    neighborhood: string;
    city: string;
    state: string;
    postal_code: string;
    coordinates?: Coordinates | null;
    census_sector_id?: string | null;
  };
  attendances: CleanedAttendance[];
}

interface ETLOptions {
  geocode?: boolean;
  cityDefault?: string;
  stateDefault?: string;
}

async function isTableExportJson(filePath: string): Promise<boolean> {
  try {
    const fd = await fsp.open(filePath, "r");
    const buf = Buffer.alloc(4096);
    const { bytesRead } = await fd.read(buf, 0, buf.length, 0);
    await fd.close();
    const sample = buf.slice(0, bytesRead).toString("utf-8");
    const sanitized = sample.replace(/\s+/g, "");
    return sanitized.startsWith('{"items":[');
  } catch {
    return false;
  }
}

function createProgressTracker(
  rs: fs.ReadStream,
  totalSize: number,
  logger: Logger,
) {
  let readBytes = 0;
  let lastLog = Date.now();
  const t0 = Date.now();
  const PROGRESS_INTERVAL_MS = 2000;

  rs.on("data", (chunk: any) => {
    readBytes += Buffer.byteLength(chunk, "utf-8");
    const now = Date.now();
    if (now - lastLog >= PROGRESS_INTERVAL_MS) {
      lastLog = now;
      if (totalSize > 0) {
        const pct = ((readBytes / totalSize) * 100).toFixed(1);
        const dt = (now - t0) / 1000;
        const mb = readBytes / (1024 * 1024);
        const speed = dt > 0 ? (mb / dt).toFixed(2) : "0.00";
        const remaining = totalSize - readBytes;
        const etaSec = Number(speed) > 0 ? remaining / (1024 * 1024) / Number(speed) : 0;
        const eta = etaSec > 0 ? `${Math.max(0, Math.round(etaSec))}s` : "—";
        logger.progress(
          `${pct}% | ${mb.toFixed(1)}MB/${(totalSize / (1024 * 1024)).toFixed(1)}MB @ ${speed} MB/s | ETA ${eta}`,
        );
      } else {
        const mb = readBytes / (1024 * 1024);
        logger.progress(`${mb.toFixed(1)}MB lidos (tamanho total desconhecido)`);
      }
    }
  });

  return {
    finalize() {
      if (totalSize > 0) {
        const mb = readBytes / (1024 * 1024);
        logger.progress(`100% | ${mb.toFixed(1)}MB/${(totalSize / (1024 * 1024)).toFixed(1)}MB`);
      }
    },
  };
}

async function streamTableExportRecords(
  filePath: string,
  logger: Logger,
  onRecord: (jsonChunk: string) => Promise<void>,
  totalSize: number,
): Promise<{ totalChunks: number; failedChunks: number }> {
  const rs = fs.createReadStream(filePath, {
    encoding: "utf-8",
    highWaterMark: 128 * 1024,
  });
  const tracker = createProgressTracker(rs, totalSize, logger);
  const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

  let total = 0;
  let failed = 0;

  const emitRecord = async (raw: string) => {
    const trimmed = raw.trim();
    if (!trimmed) return;
    total += 1;
    try {
      await onRecord(trimmed);
    } catch {
      failed += 1;
    }
  };

  for await (const line of rl) {
    let trimmed = line.trim();
    if (!trimmed) continue;
    if (trimmed === "[" || trimmed === "]" || trimmed === "],") continue;
    if (trimmed === "]}") break;

    if (trimmed.startsWith(",")) {
      trimmed = trimmed.slice(1).trim();
    }

    if (!trimmed) continue;

    const startIdx = trimmed.indexOf("{");
    const endIdx = trimmed.lastIndexOf("}");
    if (startIdx === -1 || endIdx === -1 || endIdx <= startIdx) {
      continue;
    }

    const payload = trimmed.slice(startIdx, endIdx + 1);
    if (!payload) continue;

    await emitRecord(payload);
  }

  tracker.finalize();

  return { totalChunks: total, failedChunks: failed };
}

function buildFullAddress(address: {
  street: string;
  number: string;
  complement?: string;
  neighborhood: string;
  city: string;
  state: string;
  postal_code: string;
}): string {
  const parts: string[] = [];
  if (address.street) parts.push(address.street);
  if (address.number) parts.push(address.number);
  if (address.complement) parts.push(address.complement);
  if (address.neighborhood) parts.push(address.neighborhood);
  if (address.city) parts.push(address.city);
  if (address.state) parts.push(address.state);
  if (address.postal_code) parts.push(address.postal_code);
  return parts.join(", ");
}

// ==============================
// 🧹 Limpeza de Entidades
// ==============================
async function toCleanEntities(
  row: RawPatientData,
  opts: Required<ETLOptions>,
  geocodeAddress?: GeocodeFn | null,
): Promise<{ patient: CleanedPatient; attendances: CleanedAttendance[] }> {
  const id = uuidv4();

  const birth = parseDate(row.dt_nascimento || null);
  const age = row.nr_anos != null
    ? Number(String(row.nr_anos).replace(",", "."))
    : ageFromBirthDate(birth);

  const gender = normalizeGender(row.ds_genero ?? row.ds_sexo ?? row.ie_sexo);
  const education = fixEducationLevel(row.ie_grau_instrucao);
  const marital = fixMaritalStatus(row.estado_civil);

  const street = extractStreet(row.ds_endereco);
  const number = extractNumber(row.ds_endereco);
  const complement = extractComplement(row.ds_endereco);
  const neighborhood = normalizeString(row.ds_bairro || "");
  const city = normalizeString(opts.cityDefault);
  const state = normalizeString(opts.stateDefault);
  const cep = sanitizeCEP(row.cd_cep);

  const fullAddress = buildFullAddress({
    street,
    number,
    complement,
    neighborhood,
    city,
    state,
    postal_code: cep,
  });

  const coords = geocodeAddress ? await geocodeAddress(fullAddress) : null;

  const patient: CleanedPatient = {
    id,
    nome: normalizeString(row.ds_paciente ?? row.nm_paciente ?? ""),
    birth_date: birth ? birth.toISOString() : null,
    age: Number.isFinite(age as number) ? (age as number) : null,
    gender,
    education_level: education,
    marital_status: marital,
    age_description: null,
    address: {
      street,
      number,
      complement,
      neighborhood,
      city,
      state,
      postal_code: cep,
      coordinates: coords,
      census_sector_id: null,
    },
    attendances: [],
  };

  const attendances: CleanedAttendance[] = [];
  const dtEntrada = parseDate(row.dt_entrada || null);
  const dtAlta = parseDate(row.dt_alta || null);
  if (row.ds_convenio || row.ds_setor_atendimento || dtEntrada) {
    attendances.push({
      id: uuidv4(),
      patient_id: id,
      convenio: normalizeString(row.ds_convenio || ""),
      nivel_urgencia: normalizeString(row.ds_nivel_urgencia || ""),
      setor_atendimento: normalizeString(row.ds_setor_atendimento || ""),
      cid_principal: row.cd_cid_principal ? normalizeString(row.cd_cid_principal) : null,
      proc_principal: row.ds_proc_principal ? normalizeString(row.ds_proc_principal) : null,
      motivo_alta: row.ds_motivo_alta ? normalizeString(row.ds_motivo_alta) : null,
      entrada: dtEntrada ? dtEntrada.toISOString() : new Date().toISOString(),
      alta: dtAlta ? dtAlta.toISOString() : null,
    });
  }

  patient.attendances = attendances;

  return { patient, attendances };
}

// ==============================
// 🧾 Utilitário CSV simples
// ==============================

function formatCsvValue(value: unknown): string {
  if (value === null || value === undefined) return "";
  if (typeof value === "number") {
    return Number.isFinite(value) ? String(value) : "";
  }
  const str = String(value);
  if (/[",\n\r;]/.test(str)) {
    return `"${str.replace(/"/g, '""')}"`;
  }
  return str;
}

interface CsvWriterOptions {
  append?: boolean;
  flushEvery?: number;
}

class CsvWriter {
  private stream: fs.WriteStream;
  private closed = false;
  private fd: number | null = null;
  private writesSinceSync = 0;
  private flushEvery: number;
  private ready: Promise<void>;

  constructor(
    private filePath: string,
    private columns: string[],
    options: CsvWriterOptions = {},
  ) {
    const append = options.append ?? false;
    this.flushEvery = Math.max(1, options.flushEvery ?? 250);

    const needHeader = !append || !fs.existsSync(filePath) || fs.statSync(filePath).size === 0;
    this.stream = fs.createWriteStream(filePath, {
      encoding: "utf-8",
      flags: append ? "a" : "w",
    });

    this.ready = new Promise<void>((resolve, reject) => {
      const onOpen = (fd: number) => {
        cleanup();
        this.fd = fd;
        if (needHeader && !this.closed) {
          this.stream.write(`${this.columns.join(",")}\n`);
        }
        resolve();
      };
      const onError = (err: Error) => {
        cleanup();
        reject(err);
      };
      const cleanup = () => {
        this.stream.removeListener("open", onOpen);
        this.stream.removeListener("error", onError);
      };
      this.stream.once("open", onOpen);
      this.stream.once("error", onError);
    });
  }

  private flushIfNeeded() {
    if (this.fd !== null && this.writesSinceSync >= this.flushEvery) {
      fs.fdatasyncSync(this.fd);
      this.writesSinceSync = 0;
    }
  }

  private async waitForDrain() {
    await new Promise<void>((resolve, reject) => {
      const onDrain = () => {
        cleanup();
        resolve();
      };
      const onError = (err: Error) => {
        cleanup();
        reject(err);
      };
      const onClose = () => {
        cleanup();
        resolve();
      };
      const cleanup = () => {
        this.stream.removeListener("drain", onDrain);
        this.stream.removeListener("error", onError);
        this.stream.removeListener("close", onClose);
      };
      this.stream.once("drain", onDrain);
      this.stream.once("error", onError);
      this.stream.once("close", onClose);
    });
  }

  async writeRow(record: Record<string, unknown>) {
    if (this.closed) {
      throw new Error("CsvWriter: tentativa de escrita após fechamento");
    }
    await this.ready;
    const line = this.columns
      .map((column) => formatCsvValue(record[column]))
      .join(",");
    const ok = this.stream.write(`${line}\n`);
    this.writesSinceSync += 1;
    if (!ok) {
      await this.waitForDrain();
    }
    this.flushIfNeeded();
  }

  async close() {
    if (this.closed) return;
    try {
      await this.ready;
    } catch (err) {
      // Se o stream não abriu, não há fd para sincronizar; apenas marca como fechado.
      this.closed = true;
      throw err;
    }
    this.closed = true;
    if (this.fd !== null && this.writesSinceSync > 0) {
      fs.fdatasyncSync(this.fd);
      this.writesSinceSync = 0;
    }
    await new Promise<void>((resolve, reject) => {
      const onError = (err: Error) => {
        cleanup();
        reject(err);
      };
      const onFinish = () => {
        cleanup();
        resolve();
      };
      const cleanup = () => {
        this.stream.removeListener("error", onError);
        this.stream.removeListener("finish", onFinish);
      };
      this.stream.on("error", onError);
      this.stream.on("finish", onFinish);
      this.stream.end();
    });
  }
}

// ==============================
// 🗂️ Exportador PostGIS
// ==============================

class PostgisExporter {
  private writer: CsvWriter;

  constructor(private outputPath: string, options: CsvWriterOptions = {}) {
    this.writer = new CsvWriter(outputPath, [
      "attendance_id",
      "patient_id",
      "patient_name",
      "birth_date",
      "age",
      "gender",
      "education_level",
      "marital_status",
      "street",
      "number",
      "complement",
      "neighborhood",
      "city",
      "state",
      "postal_code",
      "latitude",
      "longitude",
      "census_sector_id",
      "convenio",
      "nivel_urgencia",
      "setor_atendimento",
      "cid_principal",
      "proc_principal",
      "motivo_alta",
      "entrada",
      "alta",
    ], options);
  }

  async write(patient: CleanedPatient, attendances: CleanedAttendance[]) {
    for (const attendance of attendances) {
      const coords = patient.address.coordinates;
      await this.writer.writeRow({
        attendance_id: attendance.id,
        patient_id: patient.id,
        patient_name: patient.nome,
        birth_date: patient.birth_date,
        age: patient.age,
        gender: patient.gender,
        education_level: patient.education_level,
        marital_status: patient.marital_status,
        street: patient.address.street,
        number: patient.address.number,
        complement: patient.address.complement,
        neighborhood: patient.address.neighborhood,
        city: patient.address.city,
        state: patient.address.state,
        postal_code: patient.address.postal_code,
        latitude: coords?.latitude ?? "",
        longitude: coords?.longitude ?? "",
        census_sector_id: patient.address.census_sector_id ?? "",
        convenio: attendance.convenio,
        nivel_urgencia: attendance.nivel_urgencia,
        setor_atendimento: attendance.setor_atendimento,
        cid_principal: attendance.cid_principal ?? "",
        proc_principal: attendance.proc_principal ?? "",
        motivo_alta: attendance.motivo_alta ?? "",
        entrada: attendance.entrada,
        alta: attendance.alta ?? "",
      });
    }
  }

  async close() {
    await this.writer.close();
  }

  get path(): string {
    return this.outputPath;
  }
}

// ==============================
// 🗺️ Consolidador de Endereços
// ==============================

type AddressLike = {
  street: string;
  number: string;
  complement: string;
  neighborhood: string;
  city: string;
  state: string;
  postal_code: string;
};

const STATE_NORMALIZATION: Record<string, string> = {
  AC: "AC",
  AL: "AL",
  AP: "AP",
  AM: "AM",
  BA: "BA",
  CE: "CE",
  DF: "DF",
  ES: "ES",
  GO: "GO",
  MA: "MA",
  MT: "MT",
  MS: "MS",
  MG: "MG",
  PA: "PA",
  PB: "PB",
  PR: "PR",
  PE: "PE",
  PI: "PI",
  RJ: "RJ",
  RN: "RN",
  RS: "RS",
  RO: "RO",
  RR: "RR",
  SC: "SC",
  SP: "SP",
  SE: "SE",
  TO: "TO",
  "ACRE": "AC",
  "ALAGOAS": "AL",
  "AMAPA": "AP",
  "AMAZONAS": "AM",
  "BAHIA": "BA",
  "CEARA": "CE",
  "DISTRITO FEDERAL": "DF",
  "ESPIRITO SANTO": "ES",
  "GOIAS": "GO",
  "MARANHAO": "MA",
  "MATO GROSSO": "MT",
  "MATO GROSSO DO SUL": "MS",
  "MINAS GERAIS": "MG",
  "PARA": "PA",
  "PARAIBA": "PB",
  "PARANA": "PR",
  "PERNAMBUCO": "PE",
  "PIAUI": "PI",
  "RIO DE JANEIRO": "RJ",
  "RIO GRANDE DO NORTE": "RN",
  "RIO GRANDE DO SUL": "RS",
  "RONDONIA": "RO",
  "RORAIMA": "RR",
  "SANTA CATARINA": "SC",
  "SAO PAULO": "SP",
  "SERGIPE": "SE",
  "TOCANTINS": "TO",
};

const GENERIC_COMPLEMENT_WORDS = new Set([
  "CASA",
  "FUNDOS",
  "FRENTE",
  "TERREO",
  "TERREO 01",
  "TERREO 1",
  "TERREO 02",
  "TERREO 2",
  "LOJA",
  "ANDAR",
  "QUINTAL",
  "GARAGEM",
  "GALPAO",
  "SALA",
  "RESIDENCIAL",
  "CASA FUNDOS",
  "CASA FRENTE",
  "CASA 01",
  "CASA 1",
  "CASA 02",
  "CASA 2",
  "CASA 03",
  "CASA 3",
]);

function canonicalizeGenericSegment(value: string): string {
  if (!value) return "";
  let result = stripDiacritics(normalizeString(value)).toUpperCase();
  result = result.replace(/[^A-Z0-9\s]/g, " ");
  result = result.replace(/\s+/g, " ").trim();
  return result;
}

function canonicalizeStreet(value: string): string {
  const tokens = canonicalizeGenericSegment(value).split(" ").filter(Boolean);
  const mapped = tokens.map((token) => {
    switch (token) {
      case "R":
      case "RD":
      case "RUA":
        return "RUA";
      case "AV":
      case "AVE":
      case "AVENIDA":
        return "AVENIDA";
      case "AL":
      case "ALAMEDA":
        return "ALAMEDA";
      case "TRAV":
      case "TRAVESSA":
      case "TRV":
        return "TRAVESSA";
      case "ROD":
      case "RODOVIA":
        return "RODOVIA";
      case "PCA":
      case "PRACA":
      case "PRAC":
        return "PRACA";
      case "EST":
      case "ESTR":
      case "ESTRADA":
        return "ESTRADA";
      default:
        return token;
    }
  });
  return mapped.join(" ");
}

function canonicalizeNumber(value: string): string {
  if (!value) return "";
  let result = stripDiacritics(normalizeString(value)).toUpperCase();
  result = result.replace(/\bS\s*\/?\s*N\b/g, "SN");
  result = result.replace(/[^0-9A-Z]/g, "");
  return result || "";
}

function canonicalizeComplementForKey(value: string): string {
  if (!value) return "";
  const segment = canonicalizeGenericSegment(value);
  if (!segment) return "";
  if (GENERIC_COMPLEMENT_WORDS.has(segment)) return "";

  const aptMatch = segment.match(/AP(?:ARTAMENTO|TO|T)?\s*(\d+[A-Z]?)$/);
  if (aptMatch) return `APTO ${aptMatch[1]}`;

  const blocoMatch = segment.match(/BLOC[O0]\s*(\w+)$/);
  if (blocoMatch) return `BLOCO ${blocoMatch[1]}`;

  const casaMatch = segment.match(/CASA\s*(\d+[A-Z]?)$/);
  if (casaMatch) return `CASA ${casaMatch[1]}`;

  return segment;
}

function canonicalizeNeighborhood(value: string): string {
  return canonicalizeGenericSegment(value);
}

function canonicalizeCity(value: string): string {
  return canonicalizeGenericSegment(value);
}

function canonicalizeState(value: string): string {
  const segment = canonicalizeGenericSegment(value);
  if (!segment) return "";
  if (STATE_NORMALIZATION[segment]) return STATE_NORMALIZATION[segment];
  if (segment.length === 2) return segment;
  return segment.slice(0, 2);
}

function canonicalizePostalCode(value: string): string {
  return (value || "").replace(/\D/g, "").padStart(8, "0").slice(0, 8);
}

function buildAddressDedupKey(address: AddressLike): string {
  return [
    canonicalizeStreet(address.street),
    canonicalizeNumber(address.number),
    canonicalizeComplementForKey(address.complement),
    canonicalizeNeighborhood(address.neighborhood),
    canonicalizeCity(address.city),
    canonicalizeState(address.state),
    canonicalizePostalCode(address.postal_code),
  ].join("|");
}

function canonicalizeFreeformAddress(address: string): string {
  const tokens = canonicalizeGenericSegment(address).split(" ").filter(Boolean);
  const mapped = tokens
    .map((token) => {
      if (token === "CASA" || token === "FUNDOS" || token === "FRENTE" || token === "TERREO") return "";
      if (token === "AP" || token === "APT" || token === "APTO" || token === "APARTAMENTO") return "APTO";
      if (token === "BLOCO" || token === "BLOC0") return "BLOCO";
      if (token === "N" || token === "NO") return "";
      switch (token) {
        case "R":
        case "RD":
        case "RUA":
          return "RUA";
        case "AV":
        case "AVE":
        case "AVENIDA":
          return "AVENIDA";
        case "AL":
        case "ALAMEDA":
          return "ALAMEDA";
        case "TRAV":
        case "TRAVESSA":
        case "TRV":
          return "TRAVESSA";
        case "ROD":
        case "RODOVIA":
          return "RODOVIA";
        case "EST":
        case "ESTR":
        case "ESTRADA":
          return "ESTRADA";
        case "PCA":
        case "PRACA":
        case "PRAC":
          return "PRACA";
        default:
          return token;
      }
    })
    .filter(Boolean);
  return mapped.join(" ");
}

function chooseBetterText(current: string, incoming: string): string {
  const currentClean = normalizeString(current);
  const incomingClean = normalizeString(incoming);
  if (!incomingClean) return currentClean;
  if (!currentClean) return incomingClean;
  if (incomingClean.length > currentClean.length) return incomingClean;
  if (incomingClean.length === currentClean.length && incomingClean.localeCompare(currentClean) > 0) {
    return incomingClean;
  }
  return currentClean;
}

function complementScore(value: string): number {
  let score = value.length;
  if (/\d/.test(value)) score += 5;
  if (/AP/i.test(value)) score += 5;
  if (/BL(OC|O)/i.test(value)) score += 3;
  return score;
}

function chooseBetterComplement(current: string, incoming: string): string {
  const currentClean = normalizeString(current);
  const incomingClean = normalizeString(incoming);
  if (!incomingClean) return currentClean;
  if (!currentClean) return incomingClean;
  const currentScore = complementScore(currentClean);
  const incomingScore = complementScore(incomingClean);
  if (incomingScore > currentScore) return incomingClean;
  if (incomingScore === currentScore && incomingClean.length > currentClean.length) {
    return incomingClean;
  }
  return currentClean;
}

function createCachedGeocodeFn(base: GeocodeFn | null): GeocodeFn | null {
  if (!base) return null;
  const cache = new Map<string, Coordinates | null>();
  return async (address: string) => {
    const key = canonicalizeFreeformAddress(address);
    if (cache.has(key)) {
      return cache.get(key) ?? null;
    }
    const result = await base(address);
    cache.set(key, result ?? null);
    return result ?? null;
  };
}

interface AddressAggregate {
  key: string;
  address: {
    street: string;
    number: string;
    complement: string;
    neighborhood: string;
    city: string;
    state: string;
    postal_code: string;
  };
  occurrences: number;
  coordinates: Coordinates[];
}

interface AddressAggregatorStats {
  patientsProcessed: number;
  uniqueAddresses: number;
  uniqueAddressesWithPrefilled: number;
  prefilledCoordinateEvents: number;
  geocodeAttempts: number;
  geocodeSuccess: number;
  geocodeFailures: number;
  addressesWithCoordinates: number;
  addressesWithoutCoordinates: number;
}

class AddressAggregator {
  private entries = new Map<string, AddressAggregate>();
  private closed = false;
  private stats = {
    patientsProcessed: 0,
    uniqueAddresses: 0,
    prefilledCoordinateEvents: 0,
    geocodeAttempts: 0,
    geocodeSuccess: 0,
    geocodeFailures: 0,
  };
  private summary: AddressAggregatorStats | null = null;

  constructor(
    private outputPath: string,
    private opts: Required<ETLOptions>,
    private logger: Logger,
    private geocodeFn: GeocodeFn | null,
  ) {}

  register(patient: CleanedPatient) {
    const addr = patient.address;
    const key = this.buildKey(addr);
    const existing = this.entries.get(key);
    const isNew = !existing;
    const entry = existing ?? {
      key,
      address: {
        street: normalizeString(addr.street),
        number: normalizeString(addr.number),
        complement: normalizeString(addr.complement),
        neighborhood: normalizeString(addr.neighborhood),
        city: normalizeString(addr.city),
        state: normalizeString(addr.state),
        postal_code: normalizeString(addr.postal_code),
      },
      occurrences: 0,
      coordinates: [],
    };

    this.stats.patientsProcessed += 1;

    const printableAddress = buildFullAddress(addr);
    if (isNew) {
      this.stats.uniqueAddresses += 1;
      if (this.stats.uniqueAddresses <= 10 || this.stats.uniqueAddresses % 5000 === 0) {
        this.logger.debug(`[Stage2] Novo endereço (#${this.stats.uniqueAddresses}): ${printableAddress}`);
      }
      if (this.stats.uniqueAddresses % 10000 === 0) {
        this.logger.info(`[Stage2] ${this.stats.uniqueAddresses} endereços únicos agregados até agora.`);
      }
    }

    entry.occurrences += 1;
    entry.address.street = chooseBetterText(entry.address.street, addr.street);
    entry.address.number = chooseBetterText(entry.address.number, addr.number);
    entry.address.complement = chooseBetterComplement(entry.address.complement, addr.complement);
    entry.address.neighborhood = chooseBetterText(entry.address.neighborhood, addr.neighborhood);
    entry.address.city = chooseBetterText(entry.address.city, addr.city);
    entry.address.state = chooseBetterText(entry.address.state, addr.state).toUpperCase();
    entry.address.postal_code = chooseBetterText(entry.address.postal_code, addr.postal_code)
      .replace(/\D/g, "")
      .slice(0, 8);

    if (addr.coordinates) {
      entry.coordinates.push(addr.coordinates);
      this.stats.prefilledCoordinateEvents += 1;
      const shouldLog =
        this.stats.prefilledCoordinateEvents <= 10 || this.stats.prefilledCoordinateEvents % 5000 === 0;
      if (shouldLog) {
        this.logger.debug(
          `[Stage2] Coordenadas existentes recebidas (${printableAddress}) -> ${this.formatCoords(addr.coordinates)}`,
        );
      }
    }
    this.entries.set(key, entry);
  }

  private formatCoords(coords: Coordinates | null | undefined): string {
    if (!coords) return "N/A";
    return `${coords.latitude.toFixed(6)}, ${coords.longitude.toFixed(6)}`;
  }

  private buildKey(address: CleanedPatient["address"]): string {
    return buildAddressDedupKey({
      street: address.street,
      number: address.number,
      complement: address.complement,
      neighborhood: address.neighborhood,
      city: address.city,
      state: address.state,
      postal_code: address.postal_code,
    });
  }

  private averageCoordinates(coords: Coordinates[]): Coordinates | null {
    if (!coords.length) return null;
    const { latitude, longitude } = coords.reduce(
      (acc, cur) => {
        acc.latitude += cur.latitude;
        acc.longitude += cur.longitude;
        return acc;
      },
      { latitude: 0, longitude: 0 },
    );
    return {
      latitude: latitude / coords.length,
      longitude: longitude / coords.length,
    };
  }

  async close(): Promise<AddressAggregatorStats> {
    if (this.summary) return this.summary;
    if (this.closed) {
      return (
        this.summary ?? {
          patientsProcessed: this.stats.patientsProcessed,
          uniqueAddresses: this.stats.uniqueAddresses,
          uniqueAddressesWithPrefilled: 0,
          prefilledCoordinateEvents: this.stats.prefilledCoordinateEvents,
          geocodeAttempts: this.stats.geocodeAttempts,
          geocodeSuccess: this.stats.geocodeSuccess,
          geocodeFailures: this.stats.geocodeFailures,
          addressesWithCoordinates: 0,
          addressesWithoutCoordinates: 0,
        }
      );
    }

    this.closed = true;
    const writer = new CsvWriter(this.outputPath, [
      "address_id",
      "street",
      "number",
      "complement",
      "neighborhood",
      "city",
      "state",
      "postal_code",
      "latitude",
      "longitude",
      "occurrences",
    ]);

    const entries = Array.from(this.entries.values()).sort((a, b) => {
      const street = a.address.street.localeCompare(b.address.street);
      if (street !== 0) return street;
      const number = a.address.number.localeCompare(b.address.number);
      if (number !== 0) return number;
      return a.address.neighborhood.localeCompare(b.address.neighborhood);
    });

    this.logger.info(
      `[Stage2] Iniciando consolidação de ${entries.length} endereços únicos (pacientes analisados: ${this.stats.patientsProcessed}).`,
    );
    if (this.geocodeFn) {
      this.logger.info(
        "[Stage2] Geocodificação habilitada: tentando preencher coordenadas ausentes durante a consolidação.",
      );
    } else {
      this.logger.info("[Stage2] Geocodificação desabilitada: apenas coordenadas já existentes serão utilizadas.");
    }

    let index = 1;
    let uniquePrefilled = 0;
    let addressesWithCoords = 0;
    let addressesWithoutCoords = 0;

    for (const entry of entries) {
      const printableAddress = buildFullAddress(entry.address);
      let coords = this.averageCoordinates(entry.coordinates);
      const hadPrefilled = coords !== null;
      if (hadPrefilled) {
        uniquePrefilled += 1;
        if (uniquePrefilled <= 10 || uniquePrefilled % 1000 === 0) {
          this.logger.debug(
            `[Stage2] Mantendo coordenadas pré-existentes (${printableAddress}) -> ${this.formatCoords(coords)}`,
          );
        }
      }

      if (!coords && this.geocodeFn) {
        this.stats.geocodeAttempts += 1;
        const attemptId = this.stats.geocodeAttempts;
        const startedAt = Date.now();
        const verboseAttempt = attemptId <= 50 || attemptId % 100 === 0;
        if (verboseAttempt) {
          this.logger.debug(
            `[Stage2] Tentativa de geocodificação #${attemptId} (${entry.occurrences} ocorrências) para: ${printableAddress}`,
          );
        }
        try {
          coords = await this.geocodeFn(printableAddress);
          if (!coords) {
            this.stats.geocodeFailures += 1;
            if (verboseAttempt) {
              this.logger.warn(
                `[Stage2] Tentativa #${attemptId} não retornou coordenadas para: ${printableAddress}`,
              );
            }
          } else {
            this.stats.geocodeSuccess += 1;
            if (verboseAttempt) {
              const elapsed = Date.now() - startedAt;
              this.logger.debug(
                `[Stage2] Tentativa #${attemptId} concluída (${elapsed}ms) -> ${this.formatCoords(coords)}`,
              );
            }
          }
        } catch (err) {
          this.stats.geocodeFailures += 1;
          this.logger.error(`[Stage2] Erro ao geocodificar endereço (${printableAddress})`, err);
        }
      }

      if (coords) {
        addressesWithCoords += 1;
      } else {
        addressesWithoutCoords += 1;
      }

      await writer.writeRow({
        address_id: index,
        street: entry.address.street,
        number: entry.address.number,
        complement: entry.address.complement,
        neighborhood: entry.address.neighborhood,
        city: entry.address.city || this.opts.cityDefault,
        state: entry.address.state || this.opts.stateDefault,
        postal_code: entry.address.postal_code,
        latitude: coords?.latitude ?? "",
        longitude: coords?.longitude ?? "",
        occurrences: entry.occurrences,
      });

      index += 1;
    }

    await writer.close();
    this.logger.success(`Arquivo de endereços consolidado: ${this.outputPath}`);

    const summary: AddressAggregatorStats = {
      patientsProcessed: this.stats.patientsProcessed,
      uniqueAddresses: entries.length,
      uniqueAddressesWithPrefilled: uniquePrefilled,
      prefilledCoordinateEvents: this.stats.prefilledCoordinateEvents,
      geocodeAttempts: this.stats.geocodeAttempts,
      geocodeSuccess: this.stats.geocodeSuccess,
      geocodeFailures: this.stats.geocodeFailures,
      addressesWithCoordinates: addressesWithCoords,
      addressesWithoutCoordinates: addressesWithoutCoords,
    };
    this.summary = summary;

    this.logger.info(
      `[Stage2] Resumo: ${summary.uniqueAddresses} endereços únicos | coordenadas finais = ${summary.addressesWithCoordinates} (sem coordenadas = ${summary.addressesWithoutCoordinates}).`,
    );
    this.logger.info(
      `[Stage2] Detalhes: pacientes processados = ${summary.patientsProcessed}; coordenadas pré-existentes (eventos) = ${summary.prefilledCoordinateEvents}; endereços com coordenadas pré-existentes = ${summary.uniqueAddressesWithPrefilled}.`,
    );
    this.logger.info(
      `[Stage2] Geocodificação: tentativas = ${summary.geocodeAttempts}; sucessos = ${summary.geocodeSuccess}; falhas = ${summary.geocodeFailures}.`,
    );

    return summary;
  }
}

// ==============================
// 📦 Leitura e Parsing em STREAM (brace-aware) + progresso por bytes
// ==============================

/** Agregador “brace-aware” para NDJSON e JSON multi-linha.
 *  Junta linhas até fechar {} e [] (ignorando aspas e escapes).
 *  Emite registros completos via callback `onRecord`.
 *  Loga progresso por bytes lidos com ETA.
 */
async function streamRecords(
  filePath: string,
  logger: Logger,
  onRecord: (jsonChunk: string) => Promise<void>
): Promise<{ totalChunks: number; failedChunks: number }> {
  const stat = await fsp.stat(filePath).catch(() => null);
  const totalSize = stat?.size ?? 0;

  if (await isTableExportJson(filePath)) {
    return streamTableExportRecords(filePath, logger, onRecord, totalSize);
  }

  const rs = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });
  const tracker = createProgressTracker(rs, totalSize, logger);

  let buf = "";
  let depthCurly = 0;
  let depthSquare = 0;
  let inString = false;
  let stringChar: '"' | "'" | null = null;
  let escape = false;

  let total = 0;
  let failed = 0;

  const flushIfComplete = async () => {
    const trimmed = buf.trim();
    if (!trimmed) return;

    // Se for um array completo sozinho, passe direto
    if (trimmed.startsWith("[") && trimmed.endsWith("]") && depthCurly === 0 && depthSquare === 0) {
      try {
        // Tenta parsear como array e emite cada item
        const arr = parseCustomJSON<any[]>(trimmed);
        if (Array.isArray(arr)) {
          for (const item of arr) {
            total++;
            try {
              await onRecord(JSON.stringify(item));
            } catch {
              failed++;
            }
          }
          buf = "";
          return;
        }
      } catch {
        // Se não deu, vai tentar como chunk normal abaixo
      }
    }

    // Caso geral: um objeto/array completo
    if (depthCurly === 0 && depthSquare === 0 && /[}\]]$/.test(trimmed)) {
      total++;
      try {
        await onRecord(trimmed);
      } catch {
        failed++;
      }
      buf = "";
    }
  };

  for await (const line of rl) {
    const ln = line + "\n"; // preserva quebra
    for (let i = 0; i < ln.length; i++) {
      const ch = ln[i];
      buf += ch;

      if (escape) {
        escape = false;
        continue;
      }
      if (inString) {
        if (ch === "\\") {
          escape = true;
          continue;
        }
        if (ch === stringChar) {
          inString = false;
          stringChar = null;
        }
        continue;
      } else {
        if (ch === '"' || ch === "'") {
          inString = true;
          stringChar = ch as '"' | "'";
          continue;
        }
        if (ch === "{") depthCurly++;
        else if (ch === "}") depthCurly = Math.max(0, depthCurly - 1);
        else if (ch === "[") depthSquare++;
        else if (ch === "]") depthSquare = Math.max(0, depthSquare - 1);
      }
    }

    // Tenta flush ao final da linha
    await flushIfComplete();
  }

  // Flush final (se sobrou algo “fechado”)
  await flushIfComplete();

  tracker.finalize();

  return { totalChunks: total, failedChunks: failed };
}

// ==============================
// ✍️ Escrita em STREAM (NDJSON)
// ==============================
interface NdjsonWriterOptions {
  append?: boolean;
  flushEvery?: number;
}

class NdjsonWriter {
  private stream: fs.WriteStream;
  private fd: number | null = null;
  private flushEvery: number;
  private writesSinceSync = 0;
  private closed = false;

  constructor(private filePath: string, options: NdjsonWriterOptions = {}) {
    const append = options.append ?? false;
    this.flushEvery = Math.max(1, options.flushEvery ?? 200);

    this.stream = fs.createWriteStream(filePath, {
      encoding: "utf-8",
      flags: append ? "a" : "w",
    });
    this.stream.on("open", (fd) => {
      this.fd = fd;
    });
  }

  private flushIfNeeded() {
    if (this.fd !== null && this.writesSinceSync >= this.flushEvery) {
      fs.fdatasyncSync(this.fd);
      this.writesSinceSync = 0;
    }
  }

  private async waitForDrain() {
    await new Promise<void>((resolve, reject) => {
      const onDrain = () => {
        cleanup();
        resolve();
      };
      const onError = (err: any) => {
        cleanup();
        reject(err);
      };
      const onClose = () => {
        cleanup();
        resolve();
      };
      const cleanup = () => {
        this.stream.removeListener("drain", onDrain);
        this.stream.removeListener("error", onError);
        this.stream.removeListener("close", onClose);
      };
      this.stream.once("drain", onDrain);
      this.stream.once("error", onError);
      this.stream.once("close", onClose);
    });
  }

  async write(obj: any) {
    if (this.closed) {
      throw new Error("NdjsonWriter: tentativa de escrita após fechamento");
    }
    const json = typeof obj === "string" ? obj : JSON.stringify(obj);
    const ok = this.stream.write(`${json}\n`);
    this.writesSinceSync += 1;
    if (!ok) {
      await this.waitForDrain();
    }
    this.flushIfNeeded();
  }

  async close() {
    if (this.closed) return;
    this.closed = true;
    if (this.fd !== null && this.writesSinceSync > 0) {
      fs.fdatasyncSync(this.fd);
      this.writesSinceSync = 0;
    }
    await new Promise<void>((resolve, reject) => {
      const onError = (err: any) => {
        cleanup();
        reject(err);
      };
      const onFinish = () => {
        cleanup();
        resolve();
      };
      const cleanup = () => {
        this.stream.removeListener("error", onError);
        this.stream.removeListener("finish", onFinish);
      };
      this.stream.on("error", onError);
      this.stream.on("finish", onFinish);
      this.stream.end();
    });
  }
}

// ==============================
// 💾 Checkpoint simples para retomada
// ==============================
interface Stage1Checkpoint {
  processed: number;
  success: number;
  failed: number;
  updatedAt: string;
  completed?: boolean;
}

class CheckpointManager {
  constructor(private filePath: string) {}

  async load(): Promise<Stage1Checkpoint | null> {
    try {
      const data = await fsp.readFile(this.filePath, "utf-8");
      return parseCustomJSON<Stage1Checkpoint>(data);
    } catch (err: any) {
      if (err?.code === "ENOENT") return null;
      throw err;
    }
  }

  async save(data: Stage1Checkpoint) {
    const tmpPath = `${this.filePath}.tmp`;
    const payload = JSON.stringify({ ...data, updatedAt: new Date().toISOString() }, null, 2);
    await fsp.writeFile(tmpPath, payload, "utf-8");
    await fsp.rename(tmpPath, this.filePath);
  }

  async clear() {
    await fsp.rm(this.filePath, { force: true });
  }
}

// ==============================
// ⚙️ Classe Principal ETL (streaming end-to-end)
// ==============================
class ETLProcessor {
  private logger: Logger;
  private geocodeFn: GeocodeFn | null;

  constructor(
    private inputPath: string,
    private outputDir: string,
    private options: ETLOptions = {}
  ) {
    this.logger = new Logger(outputDir);
    const rawGeocode = this.options.geocode
      ? createGeocoder({ logger: this.logger, userAgent: process.env.GEOCODER_USER_AGENT })
      : null;
    this.geocodeFn = createCachedGeocodeFn(rawGeocode);
  }

  private get opts(): Required<ETLOptions> {
    return {
      geocode: this.options.geocode ?? false,
      cityDefault: this.options.cityDefault ?? "Itajubá",
      stateDefault: this.options.stateDefault ?? "MG",
    };
  }

  private async ensureOutputDir() {
    await fsp.mkdir(this.outputDir, { recursive: true });
  }

  async processData(): Promise<void> {
    await this.ensureOutputDir();

    const patientsPath = path.join(this.outputDir, "patients.ndjson");
    const attendancesPath = path.join(this.outputDir, "attendances.ndjson");
    const postgisPath = path.join(this.outputDir, "postgis_ready.csv");
    const checkpointPath = path.join(this.outputDir, "stage1.checkpoint.json");
    const checkpointManager = new CheckpointManager(checkpointPath);

    const existingCheckpoint = await checkpointManager.load();
    const checkpointIsValid =
      !!existingCheckpoint &&
      !existingCheckpoint.completed &&
      existingCheckpoint.processed > 0 &&
      fs.existsSync(patientsPath) &&
      fs.existsSync(attendancesPath) &&
      fs.existsSync(postgisPath);

    let resumeFrom = checkpointIsValid && existingCheckpoint ? existingCheckpoint.processed : 0;
    let success = checkpointIsValid && existingCheckpoint ? existingCheckpoint.success : 0;
    let failed = checkpointIsValid && existingCheckpoint ? existingCheckpoint.failed : 0;

    if (checkpointIsValid && existingCheckpoint) {
      this.logger.info(
        `Checkpoint encontrado (${resumeFrom} registros concluídos). Retomando a partir do registro ${resumeFrom + 1}.`,
      );
    } else {
      resumeFrom = 0;
      success = 0;
      failed = 0;
      await checkpointManager.clear();
      await Promise.all([
        fsp.rm(patientsPath, { force: true }).catch(() => undefined),
        fsp.rm(attendancesPath, { force: true }).catch(() => undefined),
        fsp.rm(postgisPath, { force: true }).catch(() => undefined),
      ]);
    }

    const appendMode = resumeFrom > 0;
    const patientsWriter = new NdjsonWriter(patientsPath, { append: appendMode });
    const attendancesWriter = new NdjsonWriter(attendancesPath, { append: appendMode });
    const postgisExporter = new PostgisExporter(postgisPath, {
      append: appendMode,
      flushEvery: 200,
    });

    this.logger.info(`Iniciando ETL com entrada: ${this.inputPath}`);

    let seen = 0;
    let processedRecords = resumeFrom;
    const checkpointInterval = 200;

    const persistCheckpoint = async (completed = false) => {
      try {
        await checkpointManager.save({
          processed: processedRecords,
          success,
          failed,
          completed,
          updatedAt: new Date().toISOString(),
        });
      } catch (err) {
        this.logger.error("Não foi possível salvar o checkpoint do Stage 1", err);
      }
    };

    const onRecord = async (jsonChunk: string) => {
      seen += 1;
      if (seen <= resumeFrom) {
        return;
      }

      processedRecords += 1;

      // parse tolerante
      let row: RawPatientData;
      try {
        row = parseCustomJSON<RawPatientData>(jsonChunk);
      } catch (e) {
        failed++;
        if (failed <= 5) this.logger.warn(`Registro ignorado (erro de parse).`);
        if (processedRecords % checkpointInterval === 0) {
          await persistCheckpoint();
        }
        return;
      }

      try {
        const { patient, attendances } = await toCleanEntities(
          row,
          this.opts,
          this.geocodeFn,
        );

        // escrita em streaming
        await patientsWriter.write(patient);
        for (const a of attendances) {
          await attendancesWriter.write(a);
        }
        await postgisExporter.write(patient, attendances);

        success++;

        // log por registros (além do progresso por bytes)
        if (processedRecords % 100 === 0) {
          this.logger.progress(
            `Registros processados: ${processedRecords} (sucesso: ${success} | falhas: ${failed})`,
          );
        }
      } catch (e) {
        failed++;
        this.logger.error(`Erro ao transformar registro #${processedRecords}`, e);
      }

      if (processedRecords % checkpointInterval === 0) {
        await persistCheckpoint();
      }
    };

    // Stream de entrada com progresso por bytes
    let totalChunks = 0;
    let failedChunks = 0;
    try {
      const result = await streamRecords(this.inputPath, this.logger, onRecord);
      totalChunks = result.totalChunks;
      failedChunks = result.failedChunks;
    } finally {
      await Promise.all([
        patientsWriter.close().catch((err) => this.logger.error("Erro ao fechar patients.ndjson", err)),
        attendancesWriter.close().catch((err) => this.logger.error("Erro ao fechar attendances.ndjson", err)),
        postgisExporter.close().catch((err) => this.logger.error("Erro ao fechar CSV PostGIS", err)),
      ]);
    }

    const total = totalChunks;
    failed += failedChunks;

    await persistCheckpoint(true);

    this.logger.success(
      `Salvo em streaming: ${patientsPath}, ${attendancesPath} e ${postgisPath}`,
    );
    this.logger.done(total, success, failed);
  }
}

// ==============================
// 🧭 Stage 2 – Consolidação de Endereços
// ==============================

async function consolidateAddresses(
  patientsPath: string,
  outputDir: string,
  options: Required<ETLOptions>,
) {
  await fsp.mkdir(outputDir, { recursive: true });
  const logger = new Logger(outputDir);
  logger.info(`Consolidando endereços a partir de: ${patientsPath}`);

  const rawGeocodeFn = options.geocode
    ? createGeocoder({ logger, userAgent: process.env.GEOCODER_USER_AGENT })
    : null;
  const geocodeFn = createCachedGeocodeFn(rawGeocodeFn);
  logger.info(
    `[Stage2] Geocodificação ${geocodeFn ? "ativada" : "desativada"} para consolidação de endereços.`,
  );

  const aggregator = new AddressAggregator(
    path.join(outputDir, "addresses.csv"),
    options,
    logger,
    geocodeFn,
  );

  let success = 0;
  let failed = 0;
  let totalChunks = 0;
  let failedChunks = 0;
  let summary: AddressAggregatorStats | null = null;

  try {
    const result = await streamRecords(patientsPath, logger, async (jsonChunk) => {
      try {
        const patient = parseCustomJSON<CleanedPatient>(jsonChunk);
        aggregator.register(patient);
        success++;
      } catch (err) {
        failed++;
        logger.error("Erro ao processar paciente para consolidação de endereços", err);
      }
    });
    totalChunks = result.totalChunks;
    failedChunks = result.failedChunks;
    logger.debug(
      `[Stage2] streamRecords finalizado: chunks=${totalChunks}, falhas=${failedChunks}, sucesso=${success}, erros=${failed}.`,
    );
  } finally {
    summary = await aggregator.close();
  }

  if (summary) {
    const summaryPath = path.join(outputDir, "stage2.summary.json");
    try {
      await fsp.writeFile(summaryPath, JSON.stringify(summary, null, 2), "utf-8");
      logger.info(`[Stage2] Sumário persistido em: ${summaryPath}`);
    } catch (err) {
      logger.error(`[Stage2] Não foi possível salvar o sumário em ${summaryPath}`, err);
    }
  }

  logger.done(totalChunks, success, failed + failedChunks);
}

// ==============================
// 🚀 Execução direta
// ==============================
async function main() {
  const args = process.argv.slice(2);

  const allowedCommands = new Set(["stage1", "stage2", "both"]);
  let command = "both";

  if (args[0] && allowedCommands.has(args[0])) {
    command = args.shift() as string;
  }

  let inputPath: string | undefined;
  let outputDir: string | undefined;
  let patientsPathOverride: string | undefined;
  let geocodePreference: boolean | undefined;

  for (const arg of args) {
    if (arg === "--geocode") {
      geocodePreference = true;
      continue;
    }
    if (arg === "--no-geocode") {
      geocodePreference = false;
      continue;
    }
    if (arg.startsWith("--patients=")) {
      patientsPathOverride = arg.slice("--patients=".length);
      continue;
    }
    if (!inputPath) {
      inputPath = arg;
    } else if (!outputDir) {
      outputDir = arg;
    }
  }

  const resolvedOutput = outputDir ?? "./output";

  const tokensAvailable = hasAnyTokens();
  const geocode = geocodePreference ?? tokensAvailable;
  if (geocodePreference === undefined) {
    console.log(
      `[INFO] Geocodificação ${geocode ? "ativada automaticamente (tokens configurados)" : "desativada (sem tokens configurados)"}.`,
    );
  } else if (geocodePreference === false) {
    console.log("[INFO] Geocodificação desativada (--no-geocode).");
  } else if (geocodePreference === true && !geocode) {
    console.log("[WARN] Geocodificação solicitada, mas nenhum token está configurado.");
  }

  const defaults: Required<ETLOptions> = {
    geocode,
    cityDefault: "Itajubá",
    stateDefault: "MG",
  };

  const runStage1 = command !== "stage2";
  const runStage2 = command !== "stage1";
  const isStage2Only = command === "stage2";

  if (runStage1) {
    const resolvedInput = inputPath ?? "./TABLE_EXPORT_DATA.json";
    const etl = new ETLProcessor(resolvedInput, resolvedOutput, defaults);
    await etl.processData();
  }

  if (runStage2) {
    const defaultPatientsPath = path.join(resolvedOutput, "patients.ndjson");
    let patientsPath = patientsPathOverride ?? (isStage2Only && inputPath ? inputPath : defaultPatientsPath);
    try {
      const stat = await fsp.stat(patientsPath);
      if (stat.isDirectory()) {
        patientsPath = path.join(patientsPath, "patients.ndjson");
      }
    } catch (err: any) {
      if (err?.code === "ENOENT" && patientsPath === inputPath && isStage2Only) {
        patientsPath = defaultPatientsPath;
      } else if (err?.code !== "ENOENT") {
        throw err;
      }
    }
    await consolidateAddresses(patientsPath, resolvedOutput, defaults);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`${color.red}Falha crítica no ETL:${color.reset}`, err);
    process.exit(1);
  });
}
