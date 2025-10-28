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
} from "./utils";
import { parseCustomJSON } from "./jsonParser";

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
  private startTime: number;

  constructor(outputDir: string) {
    this.errorPath = path.join(outputDir, "error_log.txt");
    this.startTime = Date.now();
  }

  info(msg: string) {
    console.log(`${color.blue}[INFO]${color.reset} ${msg}`);
  }

  progress(msg: string) {
    // Linha de progresso diferenciada
    console.log(`${color.gray}[PROGRESS]${color.reset} ${msg}`);
  }

  success(msg: string) {
    console.log(`${color.green}[OK]${color.reset} ${msg}`);
  }

  warn(msg: string) {
    console.warn(`${color.yellow}[WARN]${color.reset} ${msg}`);
  }

  error(msg: string, details?: any) {
    console.error(`${color.red}[ERROR]${color.reset} ${msg}`);
    const entry = `[${new Date().toISOString()}] ${msg}\n${details ? (typeof details === "string" ? details : JSON.stringify(details, null, 2)) : ""}\n\n`;
    fs.appendFileSync(this.errorPath, entry, "utf-8");
  }

  done(total: number, success: number, failed: number) {
    const elapsed = ((Date.now() - this.startTime) / 1000).toFixed(2);
    const msg = `🏁 Finalizado: ${success}/${total} registros processados com sucesso (${failed} falhas) em ${elapsed}s`;
    console.log(`${color.bold}${color.green}${msg}${color.reset}`);
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

type Coordinates = { latitude: number; longitude: number };

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
  const rs = fs.createReadStream(filePath, { encoding: "utf-8" });
  const tracker = createProgressTracker(rs, totalSize, logger);

  let lookback = "";
  let insideItems = false;
  let recordBuffer = "";
  let recordDepth = 0;
  let recordInString = false;
  let recordEscape = false;

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

  for await (const chunk of rs) {
    for (let idx = 0; idx < chunk.length; idx += 1) {
      const ch = chunk[idx];

      if (!insideItems) {
        lookback += ch;
        if (lookback.length > 128) {
          lookback = lookback.slice(-128);
        }
        const sanitized = lookback.replace(/\s+/g, "");
        if (sanitized.endsWith('"items":[')) {
          insideItems = true;
          lookback = "";
        }
        continue;
      }

      if (!recordBuffer) {
        if (ch === ']' && !recordInString && recordDepth === 0) {
          insideItems = false;
          continue;
        }
        if (ch === ',' || ch === '\n' || ch === '\r' || ch === '\t' || ch === ' ') {
          continue;
        }
      }

      recordBuffer += ch;

      if (recordEscape) {
        recordEscape = false;
        continue;
      }
      if (ch === '\\') {
        recordEscape = true;
        continue;
      }
      if (ch === '"') {
        recordInString = !recordInString;
        continue;
      }
      if (!recordInString) {
        if (ch === '{') {
          recordDepth += 1;
        } else if (ch === '}') {
          recordDepth = Math.max(0, recordDepth - 1);
          if (recordDepth === 0) {
            await emitRecord(recordBuffer);
            recordBuffer = "";
          }
        }
      }
    }
  }

  if (recordBuffer.trim()) {
    await emitRecord(recordBuffer);
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
// 🌍 Stub de Geocodificação
// ==============================
async function geocodeAddressStub(_fullAddress: string): Promise<Coordinates | null> {
  return null; // mantido como stub
}

// ==============================
// 🧹 Limpeza de Entidades
// ==============================
async function toCleanEntities(
  row: RawPatientData,
  opts: Required<ETLOptions>
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

  const coords = opts.geocode ? await geocodeAddressStub(fullAddress) : null;

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

    this.stream.on("open", (fd) => {
      this.fd = fd;
      if (needHeader) {
        this.stream.write(`${this.columns.join(",")}\n`);
      }
    });
  }

  private flushIfNeeded() {
    if (this.fd !== null && this.writesSinceSync >= this.flushEvery) {
      fs.fdatasyncSync(this.fd);
      this.writesSinceSync = 0;
    }
  }

  writeRow(record: Record<string, unknown>) {
    if (this.closed) {
      throw new Error("CsvWriter: tentativa de escrita após fechamento");
    }
    const line = this.columns
      .map((column) => formatCsvValue(record[column]))
      .join(",");
    this.stream.write(`${line}\n`);
    this.writesSinceSync += 1;
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

  write(patient: CleanedPatient, attendances: CleanedAttendance[]) {
    for (const attendance of attendances) {
      const coords = patient.address.coordinates;
      this.writer.writeRow({
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

class AddressAggregator {
  private entries = new Map<string, AddressAggregate>();
  private closed = false;

  constructor(
    private outputPath: string,
    private opts: Required<ETLOptions>,
    private logger: Logger,
  ) {}

  register(patient: CleanedPatient) {
    const addr = patient.address;
    const key = this.buildKey(addr);
    const entry = this.entries.get(key) ?? {
      key,
      address: {
        street: addr.street,
        number: addr.number,
        complement: addr.complement,
        neighborhood: addr.neighborhood,
        city: addr.city,
        state: addr.state,
        postal_code: addr.postal_code,
      },
      occurrences: 0,
      coordinates: [],
    };

    entry.occurrences += 1;
    if (addr.coordinates) {
      entry.coordinates.push(addr.coordinates);
    }
    this.entries.set(key, entry);
  }

  private buildKey(address: CleanedPatient["address"]): string {
    return [
      address.street,
      address.number,
      address.complement,
      address.neighborhood,
      address.city,
      address.state,
      address.postal_code,
    ]
      .map((value) => normalizeString(value).toUpperCase())
      .join("|");
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

  async close() {
    if (this.closed) return;
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

    let index = 1;
    for (const entry of entries) {
      let coords = this.averageCoordinates(entry.coordinates);
      if (!coords && this.opts.geocode) {
        const fullAddress = buildFullAddress(entry.address);
        try {
          coords = await geocodeAddressStub(fullAddress);
          if (!coords) {
            this.logger.warn(`Sem coordenadas para endereço: ${fullAddress}`);
          }
        } catch (err) {
          this.logger.error(`Erro ao geocodificar endereço: ${fullAddress}`, err);
        }
      }

      writer.writeRow({
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

  write(obj: any) {
    if (this.closed) {
      throw new Error("NdjsonWriter: tentativa de escrita após fechamento");
    }
    const json = typeof obj === "string" ? obj : JSON.stringify(obj);
    this.stream.write(`${json}\n`);
    this.writesSinceSync += 1;
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

  constructor(
    private inputPath: string,
    private outputDir: string,
    private options: ETLOptions = {}
  ) {
    this.logger = new Logger(outputDir);
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
        const { patient, attendances } = await toCleanEntities(row, this.opts);

        // escrita em streaming
        patientsWriter.write(patient);
        for (const a of attendances) attendancesWriter.write(a);
        postgisExporter.write(patient, attendances);

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

  const aggregator = new AddressAggregator(
    path.join(outputDir, "addresses.csv"),
    options,
    logger,
  );

  let success = 0;
  let failed = 0;
  let totalChunks = 0;
  let failedChunks = 0;

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
  } finally {
    await aggregator.close();
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
  let geocode = false;

  for (const arg of args) {
    if (arg === "--geocode") {
      geocode = true;
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
    const patientsPath = patientsPathOverride ?? (isStage2Only && inputPath ? inputPath : defaultPatientsPath);
    await consolidateAddresses(patientsPath, resolvedOutput, defaults);
  }
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`${color.red}Falha crítica no ETL:${color.reset}`, err);
    process.exit(1);
  });
}
