import {isUtf8} from 'node:buffer';

export type ArgumentHeader = {path: string[]; name: string; type: 'string' | 'integer' | 'boolean'};
export class InvalidHeaderAnnotation extends Error {}
const token = /^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$/;
const schemaMaps = new Set(['$defs', 'definitions', 'patternProperties', 'dependentSchemas', 'dependencies']);
const subschemas = new Set(['items', 'prefixItems', 'additionalItems', 'contains', 'additionalProperties', 'unevaluatedProperties',
  'unevaluatedItems', 'propertyNames', 'oneOf', 'anyOf', 'allOf', 'not', 'if', 'then', 'else', 'contentSchema']);
const object = (value: unknown): value is Record<string, unknown> => typeof value === 'object' && value !== null && !Array.isArray(value);

export function argumentHeaderDeclarations(schema: unknown): ArgumentHeader[] {
  const declarations: ArgumentHeader[] = [];
  const names = new Set<string>();
  const visit = (value: unknown, path: string[], reachable: boolean) => {
    if (!object(value)) return;
    if (Object.hasOwn(value, 'x-mcp-header')) {
      const name = value['x-mcp-header'];
      const type = value.type;
      if (!reachable || !path.length) throw new InvalidHeaderAnnotation('Header annotations must follow a chain of properties');
      if (typeof name !== 'string' || !token.test(name)) throw new InvalidHeaderAnnotation('Header annotation names must be nonempty HTTP field-name tokens');
      if (type !== 'string' && type !== 'integer' && type !== 'boolean') throw new InvalidHeaderAnnotation('Header annotations require string, integer or boolean properties');
      if (names.has(name.toLowerCase())) throw new InvalidHeaderAnnotation('Header annotation names must be unique ignoring case');
      names.add(name.toLowerCase());
      declarations.push({path, name, type});
    }
    if (object(value.properties)) for (const [key, child] of Object.entries(value.properties)) visit(child, [...path, key], reachable);
    for (const [key, child] of Object.entries(value)) {
      if (schemaMaps.has(key) && object(child)) for (const nested of Object.values(child)) visit(nested, path, false);
      if (subschemas.has(key)) {
        if (Array.isArray(child)) {
          for (const nested of child) visit(nested, path, false);
        } else visit(child, path, false);
      }
    }
  };
  visit(schema, [], true);
  return declarations;
}

function plainHeaderValue(value: string) {
  // eslint-disable-next-line no-control-regex -- HTTP field values allow horizontal tabs.
  return value.trim() === value && /^[\x09\x20-\x7e]*$/.test(value);
}
export function encodeHeaderValue(value: string): string {
  if (value.length && plainHeaderValue(value)
    && !(value.startsWith('=?base64?') && value.endsWith('?='))) return value;
  return `=?base64?${Buffer.from(value).toString('base64')}?=`;
}
export function decodeHeaderValue(value: string | null): string | undefined {
  if (value === null) return;
  if (!value.startsWith('=?base64?') || !value.endsWith('?=')) return plainHeaderValue(value) ? value : undefined;
  const encoded = value.slice(9, -2);
  const bytes = Buffer.from(encoded, 'base64');
  if (bytes.toString('base64') !== encoded || !isUtf8(bytes)) return;
  return bytes.toString('utf8');
}
export function argumentHeaders(declarations: readonly ArgumentHeader[], args: unknown): Record<string, string> {
  const headers: Record<string, string> = {};
  for (const {path, name, type} of declarations) {
    let value = args;
    for (const key of path) value = object(value) && Object.hasOwn(value, key) ? value[key] : undefined;
    if (value === undefined || value === null) continue;
    const valid = type === 'integer' ? Number.isSafeInteger(value) : typeof value === type;
    if (!valid) throw new Error('Annotated arguments must match their primitive type; integers must be within the safe range');
    headers[`Mcp-Param-${name}`] = encodeHeaderValue(String(value));
  }
  return headers;
}
