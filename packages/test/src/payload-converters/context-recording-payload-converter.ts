import type { Payload, PayloadConverter, SerializationContext } from '@temporalio/common';
import { defaultPayloadConverter } from '@temporalio/common';
import { decode, encode } from '@temporalio/common/encoding';

/** Metadata key under which the encoding context is recorded as JSON. */
export const RECORDED_CONTEXT_KEY = 'test-recorded-context';
/** Metadata key that asks `fromPayload` to return its decoding context instead of the value. */
export const ECHO_CONTEXT_KEY = 'test-echo-context';

/**
 * Records the serialization context seen by the converter, so tests can observe the context the
 * workflow sandbox passes without sharing memory with it: `toPayload` stores the context in the
 * payload metadata, and `fromPayload` returns the context for payloads marked with
 * {@link ECHO_CONTEXT_KEY}.
 */
class ContextRecordingPayloadConverter implements PayloadConverter {
  toPayload<T>(value: T, context?: SerializationContext): Payload {
    const payload = defaultPayloadConverter.toPayload(value);
    return {
      ...payload,
      metadata: { ...payload.metadata, [RECORDED_CONTEXT_KEY]: encode(JSON.stringify(context ?? null)) },
    };
  }

  fromPayload<T>(payload: Payload, context?: SerializationContext): T {
    if (payload.metadata?.[ECHO_CONTEXT_KEY] != null) return (context ?? null) as T;
    return defaultPayloadConverter.fromPayload(payload);
  }
}

export function recordedContext(payload: Payload | null | undefined): unknown {
  const raw = payload?.metadata?.[RECORDED_CONTEXT_KEY];
  return raw == null ? undefined : JSON.parse(decode(raw));
}

export const payloadConverter = new ContextRecordingPayloadConverter();
