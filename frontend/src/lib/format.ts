/**
 * Formats an ISO date ("2020-05-01", or the date part of a timestamp) as dd/mm/yyyy.
 * Reads the string itself: `new Date("2020-05-01")` is UTC midnight, which shows as the
 * previous day in Brazilian time zones.
 */
export function formatDateBR(value: string | null | undefined): string {
  const match = value?.match(/^(\d{4})-(\d{2})-(\d{2})/);
  if (!match) return "—";
  const [, year, month, day] = match;
  return `${day}/${month}/${year}`;
}
