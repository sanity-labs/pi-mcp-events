export {EventsClient, type ClientOptions, type WatchOptions, type Watch, type Observation} from './client.ts';
export {EventsClientError, EventsClientErrorCode} from './errors.ts';
export {createHttpTransport, EventsWire} from './wire.ts';
export {EVENTS_EXTENSION, PROTOCOL_VERSION, EventProtocolError, EventProtocolErrorCode, type DeliveryMode, type EventDescriptor, type EventOccurrence} from './protocol.ts';
