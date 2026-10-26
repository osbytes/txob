/**
 * Splits `$1`-style SQL into its literal chunks and the parameter index (0-based) between each
 * pair, for query builders that take parameters inline or positionally.
 */
export const splitPlaceholders = (
  text: string,
): { chunks: string[]; indexes: number[] } => {
  const chunks: string[] = [];
  const indexes: number[] = [];
  let last = 0;
  for (const match of text.matchAll(/\$(\d+)/g)) {
    chunks.push(text.slice(last, match.index));
    indexes.push(Number(match[1]) - 1);
    last = match.index! + match[0].length;
  }
  chunks.push(text.slice(last));
  return { chunks, indexes };
};

/** Formats an array as a Postgres array literal, for drivers that expand arrays into lists. */
export const toArrayLiteral = (values: unknown[]): string =>
  `{${values
    .map((value) =>
      value === null || value === undefined
        ? "NULL"
        : `"${String(value).replace(/[\\"]/g, "\\$&")}"`,
    )
    .join(",")}}`;
