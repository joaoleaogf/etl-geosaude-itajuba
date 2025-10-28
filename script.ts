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
  nr_anos?: number | string;
  dt_nascimento?: string;
  ds_genero?: string;
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
    coordinates?: { latitude: number; longitude: number } | null;
    census_sector_id?: string | null;
  };
  attendances: CleanedAttendance[];
}

interface ETLOptions {
  geocode?: boolean;
  cityDefault?: string;
  stateDefault?: string;
}

// ==============================
// 🌍 Stub de Geocodificação
// ==============================
async function geocodeAddressStub(_fullAddress: string): Promise<{ latitude: number; longitude: number } | null> {
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

  const gender = normalizeGender(row.ds_genero);
  const education = fixEducationLevel(row.ie_grau_instrucao);
  const marital = fixMaritalStatus(row.estado_civil);

  const street = extractStreet(row.ds_endereco);
  const number = extractNumber(row.ds_endereco);
  const complement = extractComplement(row.ds_endereco);
  const neighborhood = normalizeString(row.ds_bairro || "");
  const city = normalizeString(opts.cityDefault);
  const state = normalizeString(opts.stateDefault);
  const cep = sanitizeCEP(row.cd_cep);

  const fullAddress = [street, number && `, ${number}`, neighborhood && ` - ${neighborhood}`, city && `, ${city}`, state && ` - ${state}`, cep && `, ${cep}`]
    .filter(Boolean)
    .join("");

  const coords = opts.geocode ? await geocodeAddressStub(fullAddress) : null;

  const patient: CleanedPatient = {
    id,
    nome: normalizeString(row.ds_paciente || ""),
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
  const rs = fs.createReadStream(filePath, { encoding: "utf-8" });
  const rl = readline.createInterface({ input: rs, crlfDelay: Infinity });

  const stat = await fsp.stat(filePath).catch(() => null);
  const totalSize = stat?.size ?? 0;

  let buf = "";
  let depthCurly = 0;
  let depthSquare = 0;
  let inString = false;
  let stringChar: '"' | "'" | null = null;
  let escape = false;

  let total = 0;
  let failed = 0;

  // progresso por bytes
  let readBytes = 0;
  let lastLog = Date.now();
  const t0 = Date.now();
  const PROGRESS_INTERVAL_MS = 2000; // 2s

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
        logger.progress(`${pct}% | ${mb.toFixed(1)}MB/${(totalSize / (1024 * 1024)).toFixed(1)}MB @ ${speed} MB/s | ETA ${eta}`);
      } else {
        const mb = readBytes / (1024 * 1024);
        logger.progress(`${mb.toFixed(1)}MB lidos (tamanho total desconhecido)`);
      }
    }
  });

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

  // log final de progresso (100%)
  if (totalSize > 0) {
    const mb = readBytes / (1024 * 1024);
    logger.progress(`100% | ${mb.toFixed(1)}MB/${(totalSize / (1024 * 1024)).toFixed(1)}MB`);
  }

  return { totalChunks: total, failedChunks: failed };
}

// ==============================
// ✍️ Escrita em STREAM (arrays JSON)
// ==============================
class JsonArrayWriter {
  private first = true;
  private stream: fs.WriteStream;

  constructor(private filePath: string) {
    this.stream = fs.createWriteStream(filePath, { encoding: "utf-8" });
    this.stream.write("[");
  }

  write(obj: any) {
    const json = typeof obj === "string" ? obj : JSON.stringify(obj);
    if (!this.first) this.stream.write(",\n");
    else this.first = false;
    this.stream.write(json);
  }

  async close() {
    return new Promise<void>((resolve, reject) => {
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
      this.stream.end("]\n");
    });
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

    const patientsPath = path.join(this.outputDir, "patients.json");
    const attendancesPath = path.join(this.outputDir, "attendances.json");
    const patientsWriter = new JsonArrayWriter(patientsPath);
    const attendancesWriter = new JsonArrayWriter(attendancesPath);

    this.logger.info(`Iniciando ETL com entrada: ${this.inputPath}`);

    let success = 0;
    let failed = 0;
    let seen = 0;

    const onRecord = async (jsonChunk: string) => {
      // parse tolerante
      let row: RawPatientData;
      try {
        row = parseCustomJSON<RawPatientData>(jsonChunk);
      } catch (e) {
        failed++;
        if (failed <= 5) this.logger.warn(`Registro ignorado (erro de parse).`);
        return;
      }

      try {
        const { patient, attendances } = await toCleanEntities(row, this.opts);

        // escrita em streaming
        patientsWriter.write(patient);
        for (const a of attendances) attendancesWriter.write(a);

        success++;
        seen++;

        // log por registros (além do progresso por bytes)
        if (seen % 100 === 0) {
          this.logger.progress(`Registros processados: ${seen} (sucesso: ${success} | falhas: ${failed})`);
        }
      } catch (e) {
        failed++;
        this.logger.error(`Erro ao transformar registro #${seen + 1}`, e);
      }
    };

    // Stream de entrada com progresso por bytes
    const { totalChunks, failedChunks } = await streamRecords(this.inputPath, this.logger, onRecord);

    await patientsWriter.close();
    await attendancesWriter.close();

    const total = totalChunks;
    failed += failedChunks;

    this.logger.success(`Salvo em streaming: ${patientsPath} e ${attendancesPath}`);
    this.logger.done(total, success, failed);
  }
}

// ==============================
// 🚀 Execução direta
// ==============================
async function main() {
  const input = process.argv[2] ?? "./TABLE_EXPORT_DATA.json";
  const outdir = process.argv[3] ?? "./output";
  const etl = new ETLProcessor(input, outdir, {
    geocode: false,
    cityDefault: "Itajubá",
    stateDefault: "MG",
  });
  await etl.processData();
}

if (require.main === module) {
  main().catch((err) => {
    console.error(`${color.red}Falha crítica no ETL:${color.reset}`, err);
    process.exit(1);
  });
}
