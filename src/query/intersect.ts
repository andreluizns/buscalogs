// Fase 5 — Interseção eficiente de postings lists (AND lógico entre termos).

/**
 * Calcula a interseção de múltiplas listas de docId. Começa pela lista
 * mais curta e usa um Set para as comparações seguintes, com saída
 * antecipada assim que o resultado esvazia — evita trabalho desnecessário
 * quando um termo raro já reduz drasticamente o conjunto de candidatos.
 */
export function intersectDocIds(postingsLists: string[][]): string[] {
  if (postingsLists.length === 0) return [];

  const sorted = [...postingsLists].sort((a, b) => a.length - b.length);
  let result = new Set(sorted[0]);

  for (let i = 1; i < sorted.length && result.size > 0; i++) {
    const next = new Set<string>();
    for (const id of sorted[i]) {
      if (result.has(id)) next.add(id);
    }
    result = next;
  }

  return [...result];
}
