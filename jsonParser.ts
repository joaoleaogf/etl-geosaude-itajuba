/* jsonParser.ts
 * Parser tolerante a registros “quebrados” (encoding irregular, vírgulas sobrando,
 * aspas faltando em chaves, etc.). Ideal para recuperar dumps imperfeitos.
 */

export interface ParsingStats {
  total: number;
  success: number;
  errors: Record<string, number>;
}

/** Limpa string JSON removendo controles e corrigindo corrupções comuns */
function cleanJsonString(input: string): string {
  let s = String(input);

  // Remove caracteres de controle (mantém \t \n)
  s = s.replace(/[\x00-\x09\x0B-\x1F\x7F-\x9F]/g, " ");

  // Correções heurísticas de encoding corrompido
  const fixes: Array<[RegExp, string]> = [
    [/N�l/gi, "Nível"],
    [/N�o/gi, "Não"],
    [/��o/gi, "ção"],
    [/�o/gi, "ão"],
    [/�/g, ""],
  ];
  for (const [re, rep] of fixes) s = s.replace(re, rep);

  // Remove vírgulas finais antes de fechar objetos/listas
  s = s.replace(/,\s*([}\]])/g, "$1");

  // Troca aspas simples por duplas quando parecem delimitar strings
  // (conservador para evitar casos dentro de texto)
  s = s.replace(/:'([^'\n\r]*)'/g, ':"$1"');

  // Garante aspas nas chaves simples
  s = s.replace(/([{,]\s*)([A-Za-z_][\w]*)(\s*:)/g, '$1"$2"$3');

  // Aparar espaços redundantes
  s = s.replace(/\s+/g, " ").trim();

  return s;
}

/** Faz uma tentativa robusta de JSON.parse após limpeza */
export function parseCustomJSON<T = any>(raw: string): T {
  // Caso já seja um objeto serializado corretamente
  try {
    return JSON.parse(raw);
  } catch {
    // continua para limpeza
  }

  let s = cleanJsonString(raw);

  // Garante abertura/fechamento
  if (!s.startsWith("{") && !s.startsWith("[")) {
    s = "{" + s;
  }
  if (!s.endsWith("}") && !s.endsWith("]")) {
    s = s + "}";
  }

  // Últimas tentativas de reparo:
  // - balanceamento básico de chaves
  const openCurly = (s.match(/{/g) || []).length;
  const closeCurly = (s.match(/}/g) || []).length;
  if (openCurly > closeCurly) s += "}".repeat(openCurly - closeCurly);

  const openBrack = (s.match(/\[/g) || []).length;
  const closeBrack = (s.match(/\]/g) || []).length;
  if (openBrack > closeBrack) s += "]".repeat(openBrack - closeBrack);

  try {
    return JSON.parse(s);
  } catch (e) {
    // Último fallback: tenta isolar primeiro objeto/array válido
    const firstObj = s.indexOf("{");
    const lastObj = s.lastIndexOf("}");
    if (firstObj >= 0 && lastObj > firstObj) {
      const slice = s.slice(firstObj, lastObj + 1);
      try {
        return JSON.parse(slice);
      } catch {
        // segue
      }
    }
    const firstArr = s.indexOf("[");
    const lastArr = s.lastIndexOf("]");
    if (firstArr >= 0 && lastArr > firstArr) {
      const slice = s.slice(firstArr, lastArr + 1);
      try {
        return JSON.parse(slice);
      } catch {
        // segue
      }
    }
    throw new Error(`parseCustomJSON: não foi possível parsear.\nTrecho: ${s.slice(0, 200)}…`);
  }
}
