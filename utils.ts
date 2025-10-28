/* utils.ts
 * Utilitários puros (sem libs externas) para normalização de strings, mapeamentos,
 * parsing de datas (pt-BR), extrações e validações.
 */

/** Normaliza texto:
 * - Remove caracteres de controle
 * - Corrige sequências corrompidas comuns (� etc.)
 * - Aplica NFC (Unicode) e trim
 */
export function normalizeString(str: string | null | undefined): string {
  if (!str) return "";
  let s = String(str);

  // Remove controls
  s = s.replace(/[\x00-\x1F\x7F-\x9F]/g, " ");

  // Correções comuns de corrupção de encoding (heurísticas)
  const fixes: Array<[RegExp, string]> = [
    [/N�l/gi, "Nível"],
    [/N�o/gi, "Não"],
    [/��o/gi, "ção"],
    [/�o/gi, "ão"],
    [/�/g, ""], // remove genérico quando não sabemos
  ];
  for (const [re, r] of fixes) s = s.replace(re, r);

  // Normalização Unicode e limpeza de espaços
  s = s.normalize("NFC").replace(/\s+/g, " ").trim();
  return s;
}

/** Remove diacríticos mantendo letras base */
export function stripDiacritics(str: string): string {
  return normalizeString(str)
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

/** Converte strings variadas de escolaridade em rótulos padronizados */
export function fixEducationLevel(raw: string | null | undefined): string {
  const s = stripDiacritics(raw || "").toUpperCase();

  // Mapas heurísticos
  const table: Array<[RegExp, string]> = [
    [/^ANALFABETO|SEM INSTRUCAO|NAO ALFABETIZADO?$/, "ANALFABETO"],
    [/(FUNDAMENTAL|PRIMARIO)/, "FUNDAMENTAL"],
    [/(MEDIO|SEGUNDO GRAU|COLEGIAL)/, "MEDIO"],
    [/(SUPERIOR|GRADUACAO|BACHAREL|LICENCIATURA)/, "SUPERIOR"],
    [/(POS|ESPECIALIZACAO|MBA|MESTR|DOUTOR)/, "POS_GRADUACAO"],
  ];

  for (const [re, out] of table) if (re.test(s)) return out;
  return s || "NAO_INFORMADO";
}

/** Converte estado civil em rótulos padronizados */
export function fixMaritalStatus(raw: string | null | undefined): string {
  const s = stripDiacritics(raw || "").toUpperCase();

  const table: Array<[RegExp, string]> = [
    [/^SOLTEIR[OA]$/, "SOLTEIRO"],
    [/^CASAD[OA]$/, "CASADO"],
    [/^DIVORCIAD[OA]|SEPARAD[OA]$/, "DIVORCIADO"],
    [/^VIUV[OA]$/, "VIUVO"],
    [/(UNIAO ESTAVEL|CONCUBINATO)/, "UNIAO_ESTAVEL"],
  ];
  for (const [re, out] of table) if (re.test(s)) return out;
  return s || "NAO_INFORMADO";
}

/** Tenta parsear datas nos formatos comuns do BR e ISO */
export function parseDate(input: string | Date | null | undefined): Date | null {
  if (!input) return null;
  if (input instanceof Date) return isNaN(+input) ? null : input;

  const s = normalizeString(String(input));

  // ISO direto
  const iso = Date.parse(s);
  if (!isNaN(iso)) return new Date(iso);

  // dd/MM/yyyy HH:mm[:ss]
  const m1 = s.match(
    /^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (m1) {
    const [, d, mo, y, hh = "0", mm = "0", ss = "0"] = m1;
    const dt = new Date(
      Number(y),
      Number(mo) - 1,
      Number(d),
      Number(hh),
      Number(mm),
      Number(ss)
    );
    return isNaN(+dt) ? null : dt;
  }

  // yyyy-MM-dd HH:mm[:ss]
  const m2 = s.match(
    /^(\d{4})-(\d{1,2})-(\d{1,2})(?:[ T](\d{1,2}):(\d{2})(?::(\d{2}))?)?$/
  );
  if (m2) {
    const [, y, mo, d, hh = "0", mm = "0", ss = "0"] = m2;
    const dt = new Date(
      Number(y),
      Number(mo) - 1,
      Number(d),
      Number(hh),
      Number(mm),
      Number(ss)
    );
    return isNaN(+dt) ? null : dt;
  }

  return null;
}

/** Extrai número (logradouro) de um endereço livre */
export function extractNumber(address: string | null | undefined): string {
  const s = normalizeString(address || "");
  const m = s.match(/\b(\d{1,6})(?:[-A-Za-z])?\b/);
  return m ? m[1] : "";
}

/** Extrai nome da rua sem número/complemento */
export function extractStreet(address: string | null | undefined): string {
  let s = normalizeString(address || "");
  s = s.replace(/\b(\d{1,6})(?:[-\w])?\b.*/g, "").trim(); // remove a partir do número
  return s.replace(/[,;-]\s*$/, "").trim();
}

/** Extrai complemento após o número (apt, bloco, etc.) */
export function extractComplement(address: string | null | undefined): string {
  const s = normalizeString(address || "");
  const m = s.match(/\b\d{1,6}([^\d].*)$/);
  return m ? normalizeString(m[1].replace(/^[,\s-]+/, "")) : "";
}

/** Calcula idade em anos (trunca) a partir da data de nascimento */
export function ageFromBirthDate(birthDate: Date | null): number | null {
  if (!birthDate) return null;
  const now = new Date();
  let age = now.getFullYear() - birthDate.getFullYear();
  const m = now.getMonth() - birthDate.getMonth();
  if (m < 0 || (m === 0 && now.getDate() < birthDate.getDate())) age--;
  return age < 0 || age > 130 ? null : age;
}

/** Gera UUID v4 simples (sem libs) */
export function uuidv4(): string {
  // Não-criptográfico, suficiente para ID local de processamento
  return "xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx".replace(/[xy]/g, (c) => {
    const r = (Math.random() * 16) | 0;
    const v = c === "x" ? r : (r & 0x3) | 0x8;
    return v.toString(16);
  });
}

/** CEP com somente dígitos e padding para 8 */
export function sanitizeCEP(cep: string | null | undefined): string {
  return (cep || "").replace(/\D/g, "").padStart(8, "0").slice(0, 8);
}

/** Gênero padronizado a partir de rótulos variados */
export function normalizeGender(raw: string | null | undefined): string {
  const s = stripDiacritics(raw || "").toUpperCase();
  if (/^M(ASCULINO)?$/.test(s)) return "MASCULINO";
  if (/^F(EMININO)?$/.test(s)) return "FEMININO";
  if (/(OUTRO|NAO INFORMADO|IGNORADO)/.test(s)) return "NAO_INFORMADO";
  return s || "NAO_INFORMADO";
}
