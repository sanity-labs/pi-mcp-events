import {Ajv as AjvDraft7, type ValidateFunction} from 'ajv';
import {Ajv2020} from 'ajv/dist/2020.js';
import {Ajv2019} from 'ajv/dist/2019.js';
import {default as formatsModule} from 'ajv-formats';
import type {EventDescriptor, EventOccurrence} from './protocol.ts';

const addFormats = formatsModule.default ?? formatsModule;

function compile(schema: Record<string, unknown>): ValidateFunction {
  const options = {strict: false, allErrors: false, validateFormats: true, addUsedSchema: false};
  const dialect = schema.$schema;
  let ajv;
  if (dialect === 'http://json-schema.org/draft-07/schema#') ajv = new AjvDraft7(options);
  else if (dialect === 'https://json-schema.org/draft/2019-09/schema') ajv = new Ajv2019(options);
  else if (dialect === undefined || dialect === 'https://json-schema.org/draft/2020-12/schema') ajv = new Ajv2020(options);
  else throw new Error('Unsupported event JSON Schema dialect');
  addFormats(ajv);
  return ajv.compile(schema);
}

export class EventContract {
  readonly descriptor: EventDescriptor;
  private readonly input: ValidateFunction;
  private readonly payload: ValidateFunction;
  constructor(descriptor: EventDescriptor) {
    this.descriptor = descriptor;
    this.input = compile(descriptor.inputSchema);
    this.payload = compile(descriptor.payloadSchema);
  }
  arguments(value: Record<string, unknown>) {
    if (!this.input(value)) throw new Error('Subscription arguments do not match inputSchema');
  }
  event(value: EventOccurrence) {
    if (value.name !== this.descriptor.name) throw new Error('Event name does not match the subscription');
    if (!this.payload(value.data)) throw new Error('Event data does not match payloadSchema');
  }
}
