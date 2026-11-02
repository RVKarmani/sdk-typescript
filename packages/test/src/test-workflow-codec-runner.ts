import test from 'ava';
import type { Payload, PayloadCodec, SerializationContext } from '@temporalio/common';
import { ApplicationFailure, defaultFailureConverter, defaultPayloadConverter } from '@temporalio/common';
import { walkPayloadsInMessage } from '@temporalio/common/internal/internal-non-workflow';
import { coresdk } from '@temporalio/proto';
import { WorkflowCodecRunner } from '@temporalio/worker/internal';
import { FreePayloadCodec, makeContextTrace, nexusCtx } from './payload-converters/serialization-context-converter';

function payload(label: string): Payload {
  return defaultPayloadConverter.toPayload(makeContextTrace(label));
}

function traceFromPayload(payload: Payload | null | undefined): string[] {
  return payload ? defaultPayloadConverter.fromPayload<{ trace: string[] }>(payload).trace : [];
}

function failureWithDetail(label: string) {
  return defaultFailureConverter.errorToFailure(
    ApplicationFailure.nonRetryable('boom', 'TestFailure', makeContextTrace(label)),
    defaultPayloadConverter
  );
}

function decodeCompletion(
  completion: coresdk.workflow_completion.IWorkflowActivationCompletion
): coresdk.workflow_completion.WorkflowActivationCompletion {
  const bytes = coresdk.workflow_completion.WorkflowActivationCompletion.encodeDelimited(completion).finish();
  return coresdk.workflow_completion.WorkflowActivationCompletion.decodeDelimited(bytes);
}

test('decodeActivation binds workflow codec context for initializeWorkflow payloads', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const activation = {
    runId: 'run-1',
    jobs: [
      {
        initializeWorkflow: {
          workflowId: 'wf-1',
          workflowType: 'test',
          randomnessSeed: { toBytes: () => new Uint8Array([1]) } as any,
          firstExecutionRunId: 'run-1',
          attempt: 1,
          startTime: {} as any,
          arguments: [payload('wf-input')],
        },
      },
    ],
  };
  const decoded = await runner.decodeActivation(activation);

  t.deepEqual(traceFromPayload(decoded.jobs?.[0]?.initializeWorkflow?.arguments?.[0] as Payload), [
    'codec.decode.bound|wf-input|workflow.default.wf-1',
  ]);
  t.deepEqual(traceFromPayload(activation.jobs[0]?.initializeWorkflow?.arguments?.[0] as Payload), []);
});

test('decodeActivation covers every initializeWorkflow payload field but headers and search attributes', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const decoded = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        initializeWorkflow: {
          workflowId: 'wf-1',
          workflowType: 'test',
          randomnessSeed: { toBytes: () => new Uint8Array([1]) } as any,
          firstExecutionRunId: 'run-1',
          attempt: 1,
          startTime: {} as any,
          headers: { 'header-key': payload('header') },
          continuedFailure: failureWithDetail('continued-failure'),
          memo: { fields: { 'memo-key': payload('memo') } },
          lastCompletionResult: { payloads: [payload('last-completion')] },
          searchAttributes: { indexedFields: { 'attr-key': payload('search-attribute') } },
        },
      },
    ],
  });

  const initializeWorkflow = decoded.jobs?.[0]?.initializeWorkflow;
  t.deepEqual(
    traceFromPayload(initializeWorkflow?.continuedFailure?.applicationFailureInfo?.details?.payloads?.[0] as Payload),
    ['codec.decode.bound|continued-failure|workflow.default.wf-1']
  );
  t.deepEqual(traceFromPayload(initializeWorkflow?.memo?.fields?.['memo-key'] as Payload), [
    'codec.decode.bound|memo|workflow.default.wf-1',
  ]);
  t.deepEqual(traceFromPayload(initializeWorkflow?.lastCompletionResult?.payloads?.[0] as Payload), [
    'codec.decode.bound|last-completion|workflow.default.wf-1',
  ]);
  // Headers and search attributes are converted by the workflow itself, never by the codec.
  t.deepEqual(traceFromPayload(initializeWorkflow?.headers?.['header-key'] as Payload), []);
  t.deepEqual(traceFromPayload(initializeWorkflow?.searchAttributes?.indexedFields?.['attr-key'] as Payload), []);
});

test('decodeActivation decodes query, update, and signal inputs with workflow context', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const decoded = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      { queryWorkflow: { queryId: 'q-1', queryType: 'q', arguments: [payload('query-input')] } },
      { doUpdate: { id: 'u-1', name: 'u', input: [payload('update-input')] } },
      { signalWorkflow: { signalName: 's', input: [payload('signal-input')] } },
    ],
  });

  t.deepEqual(traceFromPayload(decoded.jobs?.[0]?.queryWorkflow?.arguments?.[0] as Payload), [
    'codec.decode.bound|query-input|workflow.default.wf-1',
  ]);
  t.deepEqual(traceFromPayload(decoded.jobs?.[1]?.doUpdate?.input?.[0] as Payload), [
    'codec.decode.bound|update-input|workflow.default.wf-1',
  ]);
  t.deepEqual(traceFromPayload(decoded.jobs?.[2]?.signalWorkflow?.input?.[0] as Payload), [
    'codec.decode.bound|signal-input|workflow.default.wf-1',
  ]);
});

test('decodeActivation decodes resolveNexusOperationStart failures', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const decoded = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        resolveNexusOperationStart: {
          seq: 8,
          failed: failureWithDetail('nexus-start-failure'),
        },
      },
    ],
  });

  t.deepEqual(
    traceFromPayload(
      decoded.jobs?.[0]?.resolveNexusOperationStart?.failed?.applicationFailureInfo?.details?.payloads?.[0] as Payload
    ),
    ['codec.decode.bound|nexus-start-failure|workflow.default.wf-1']
  );
});

test('encodeCompletion stores activity context and decodeActivation reuses it for resolveActivity', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const encoded = decodeCompletion(
    await runner.encodeCompletion({
      successful: {
        commands: [
          {
            scheduleActivity: {
              seq: 1,
              activityId: 'act-1',
              arguments: [payload('activity-input')],
            },
          },
        ],
      },
    })
  );

  t.deepEqual(traceFromPayload(encoded.successful?.commands?.[0]?.scheduleActivity?.arguments?.[0] as Payload), [
    'codec.encode.bound|activity-input|activity.default.wf-1.act-1.false',
  ]);

  const decoded = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        resolveActivity: {
          seq: 1,
          result: {
            completed: {
              result: payload('activity-output'),
            },
          },
        },
      },
    ],
  });

  t.deepEqual(traceFromPayload(decoded.jobs?.[0]?.resolveActivity?.result?.completed?.result as Payload), [
    'codec.decode.bound|activity-output|activity.default.wf-1.act-1.false',
  ]);
});

test('encodeCompletion encodes event group marker labels with the command context', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const encoded = decodeCompletion(
    await runner.encodeCompletion({
      successful: {
        commands: [
          {
            scheduleActivity: {
              seq: 1,
              activityId: 'act-1',
              arguments: [payload('activity-input')],
            },
            eventGroupMarkers: [{ label: { id: 'group-1', label: payload('activity-marker') } }],
          },
          {
            completeWorkflowExecution: {},
            eventGroupMarkers: [{ label: { id: 'group-2', label: payload('workflow-marker') } }],
          },
        ],
      },
    })
  );

  const commands = encoded.successful?.commands ?? [];
  t.deepEqual(traceFromPayload(commands[0]?.eventGroupMarkers?.[0]?.label?.label as Payload), [
    'codec.encode.bound|activity-marker|activity.default.wf-1.act-1.false',
  ]);
  // A command that targets nothing of its own leaves its markers on the workflow context.
  t.deepEqual(traceFromPayload(commands[1]?.eventGroupMarkers?.[0]?.label?.label as Payload), [
    'codec.encode.bound|workflow-marker|workflow.default.wf-1',
  ]);
});

test('encodeCompletion keeps distinct child-workflow contexts for start and completion', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  await runner.encodeCompletion({
    successful: {
      commands: [
        {
          startChildWorkflowExecution: {
            seq: 2,
            workflowId: 'child-1',
            input: [payload('child-input')],
          },
        },
      ],
    },
  });

  const cancelledStart = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        resolveChildWorkflowExecutionStart: {
          seq: 2,
          cancelled: {
            failure: failureWithDetail('child-start-failure'),
          },
        },
      },
    ],
  });

  t.deepEqual(
    traceFromPayload(
      cancelledStart.jobs?.[0]?.resolveChildWorkflowExecutionStart?.cancelled?.failure?.applicationFailureInfo?.details
        ?.payloads?.[0] as Payload
    ),
    ['codec.decode.bound|child-start-failure|workflow.default.child-1']
  );

  await runner.encodeCompletion({
    successful: {
      commands: [
        {
          startChildWorkflowExecution: {
            seq: 3,
            workflowId: 'child-2',
            input: [payload('child-input-2')],
          },
        },
      ],
    },
  });

  const completedChild = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        resolveChildWorkflowExecution: {
          seq: 3,
          result: {
            completed: {
              result: payload('child-output'),
            },
          },
        },
      },
    ],
  });

  t.deepEqual(
    traceFromPayload(completedChild.jobs?.[0]?.resolveChildWorkflowExecution?.result?.completed?.result as Payload),
    ['codec.decode.bound|child-output|workflow.default.child-2']
  );
});

test('signal and cancel external workflow paths use target workflow context', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  await runner.encodeCompletion({
    successful: {
      commands: [
        {
          signalExternalWorkflowExecution: {
            seq: 4,
            args: [payload('signal-input')],
            workflowExecution: { workflowId: 'target-wf' },
          },
        },
        {
          requestCancelExternalWorkflowExecution: {
            seq: 5,
            workflowExecution: { workflowId: 'target-wf' },
          },
        },
      ],
    },
  });

  const decodedSignal = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        resolveSignalExternalWorkflow: {
          seq: 4,
          failure: failureWithDetail('signal-failure'),
        },
      },
      {
        resolveRequestCancelExternalWorkflow: {
          seq: 5,
          failure: failureWithDetail('cancel-failure'),
        },
      },
    ],
  });

  t.deepEqual(
    traceFromPayload(
      decodedSignal.jobs?.[0]?.resolveSignalExternalWorkflow?.failure?.applicationFailureInfo?.details
        ?.payloads?.[0] as Payload
    ),
    ['codec.decode.bound|signal-failure|workflow.default.target-wf']
  );
  t.deepEqual(
    traceFromPayload(
      decodedSignal.jobs?.[1]?.resolveRequestCancelExternalWorkflow?.failure?.applicationFailureInfo?.details
        ?.payloads?.[0] as Payload
    ),
    ['codec.decode.bound|cancel-failure|workflow.default.target-wf']
  );
});

function nexusRunner(): WorkflowCodecRunner {
  return new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });
}

function scheduleNexusOperation(seq: number) {
  return {
    successful: {
      commands: [
        {
          scheduleNexusOperation: {
            seq,
            endpoint: 'my-endpoint',
            service: 'my-service',
            operation: 'my-operation',
            input: payload('nexus-input'),
          },
          userMetadata: { summary: payload('nexus-summary') },
        },
      ],
    },
  };
}

const nexusOperationCtx = nexusCtx('my-endpoint', 'my-service', 'my-operation');

test('nexus operation paths use the operation context', async (t) => {
  const runner = nexusRunner();

  const encoded = decodeCompletion(await runner.encodeCompletion(scheduleNexusOperation(6)));
  t.deepEqual(traceFromPayload(encoded.successful?.commands?.[0]?.scheduleNexusOperation?.input as Payload), [
    `codec.encode.bound|nexus-input|${nexusOperationCtx}`,
  ]);
  // User metadata is not part of the Nexus start request, so it stays on the caller's context.
  t.deepEqual(traceFromPayload(encoded.successful?.commands?.[0]?.userMetadata?.summary as Payload), [
    'codec.encode.bound|nexus-summary|workflow.default.wf-1',
  ]);

  // An asynchronous start is not terminal, so it must leave the context for the resolution below.
  await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveNexusOperationStart: { seq: 6, operationToken: 'token-1' } }],
  });

  const decodedCompleted = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveNexusOperation: { seq: 6, result: { completed: payload('nexus-output') } } }],
  });
  t.deepEqual(traceFromPayload(decodedCompleted.jobs?.[0]?.resolveNexusOperation?.result?.completed as Payload), [
    `codec.decode.bound|nexus-output|${nexusOperationCtx}`,
  ]);
});

test('a resolution with no scheduled nexus operation falls back to workflow context', async (t) => {
  const runner = nexusRunner();

  const decodedFailed = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveNexusOperation: { seq: 7, result: { failed: failureWithDetail('nexus-failure') } } }],
  });

  t.deepEqual(
    traceFromPayload(
      decodedFailed.jobs?.[0]?.resolveNexusOperation?.result?.failed?.applicationFailureInfo?.details
        ?.payloads?.[0] as Payload
    ),
    ['codec.decode.bound|nexus-failure|workflow.default.wf-1']
  );
});

test('a failed nexus start uses the operation context and then releases it', async (t) => {
  const runner = nexusRunner();
  await runner.encodeCompletion(scheduleNexusOperation(8));

  const decodedStartFailure = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveNexusOperationStart: { seq: 8, failed: failureWithDetail('nexus-start-failure') } }],
  });
  t.deepEqual(
    traceFromPayload(
      decodedStartFailure.jobs?.[0]?.resolveNexusOperationStart?.failed?.applicationFailureInfo?.details
        ?.payloads?.[0] as Payload
    ),
    [`codec.decode.bound|nexus-start-failure|${nexusOperationCtx}`]
  );

  // White-box probe of the pending-context map, not a realistic history: Core never sends a
  // ResolveNexusOperation after a failed start. Falling back to workflow context here is how an
  // outside observer can tell the terminal start released its entry instead of retaining it.
  const probe = await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveNexusOperation: { seq: 8, result: { completed: payload('nexus-output') } } }],
  });
  t.deepEqual(traceFromPayload(probe.jobs?.[0]?.resolveNexusOperation?.result?.completed as Payload), [
    'codec.decode.bound|nexus-output|workflow.default.wf-1',
  ]);
});

test('runner remains compatible with codecs that ignore context', async (t) => {
  class FreeOnlyCodec implements PayloadCodec {
    async encode(payloads: Payload[]): Promise<Payload[]> {
      return payloads.map((payload) => {
        const value = defaultPayloadConverter.fromPayload<{ label: string; trace: string[] }>(payload);
        value.trace.push(`codec.encode.free|${value.label}`);
        return defaultPayloadConverter.toPayload(value);
      });
    }

    async decode(payloads: Payload[]): Promise<Payload[]> {
      return payloads.map((payload) => {
        const value = defaultPayloadConverter.fromPayload<{ label: string; trace: string[] }>(payload);
        value.trace.push(`codec.decode.free|${value.label}`);
        return defaultPayloadConverter.toPayload(value);
      });
    }
  }

  const runner = new WorkflowCodecRunner([new FreeOnlyCodec()], {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'wf-1',
  });

  const encoded = decodeCompletion(
    await runner.encodeCompletion({
      successful: {
        commands: [
          {
            completeWorkflowExecution: {
              result: payload('wf-output'),
            },
          },
        ],
      },
    })
  );

  t.deepEqual(traceFromPayload(encoded.successful?.commands?.[0]?.completeWorkflowExecution?.result as Payload), [
    'codec.encode.free|wf-output',
  ]);
});

test('generic protobuf payload walking rejects an unrecognized root', (t) => {
  t.throws(() => walkPayloadsInMessage({ $type: { fullName: '.example.Unknown' } }, {} as any, undefined), {
    message: 'Unknown root message type: example.Unknown',
  });
});

test('ordinary Nexus calls with a System Nexus service name are not rewritten', async (t) => {
  const runner = new WorkflowCodecRunner([new FreePayloadCodec()], {
    type: 'workflow',
    namespace: 'caller-ns',
    workflowId: 'caller-id',
  });
  const encoded = decodeCompletion(
    await runner.encodeCompletion({
      successful: {
        commands: [
          {
            scheduleNexusOperation: {
              seq: 43,
              endpoint: 'ordinary-endpoint',
              service: 'temporal.api.workflowservice.v1.WorkflowService',
              operation: 'SignalWithStartWorkflowExecution',
              input: payload('ordinary-nexus-input'),
            },
          },
        ],
      },
    })
  );
  const envelope = encoded.successful?.commands?.[0]?.scheduleNexusOperation?.input;
  t.not(envelope?.metadata?.encoding?.[0], 98);
  t.deepEqual(traceFromPayload(envelope), [
    `codec.encode.bound|ordinary-nexus-input|${nexusCtx(
      'ordinary-endpoint',
      'temporal.api.workflowservice.v1.WorkflowService',
      'SignalWithStartWorkflowExecution'
    )}`,
  ]);
});

/** Records the full context the codec receives for each labelled payload, keyed by operation and label. */
class RecordingPayloadCodec implements PayloadCodec {
  readonly contexts = new Map<string, SerializationContext | undefined>();

  async encode(payloads: Payload[], context?: SerializationContext): Promise<Payload[]> {
    return this.record('encode', payloads, context);
  }

  async decode(payloads: Payload[], context?: SerializationContext): Promise<Payload[]> {
    return this.record('decode', payloads, context);
  }

  private record(op: string, payloads: Payload[], context: SerializationContext | undefined): Payload[] {
    for (const p of payloads) {
      const { label } = defaultPayloadConverter.fromPayload<{ label: string }>(p);
      this.contexts.set(`${op}|${label}`, context);
    }
    return payloads;
  }
}

const parentContext = {
  type: 'workflow' as const,
  namespace: 'default',
  workflowId: 'wf-1',
  workflowType: 'ParentWorkflow',
};

test('activity contexts carry activity and parent workflow types across schedule and out-of-order resolution', async (t) => {
  const codec = new RecordingPayloadCodec();
  const runner = new WorkflowCodecRunner([codec], parentContext);

  await runner.encodeCompletion({
    successful: {
      commands: [
        { scheduleActivity: { seq: 1, activityId: 'a-1', activityType: 'charge', arguments: [payload('in-1')] } },
        {
          scheduleLocalActivity: { seq: 2, activityId: '2', activityType: 'lookup', arguments: [payload('in-2')] },
        },
      ],
    },
  });
  const expected1 = {
    type: 'activity',
    namespace: 'default',
    workflowId: 'wf-1',
    workflowType: 'ParentWorkflow',
    activityId: 'a-1',
    activityType: 'charge',
    isLocal: false,
  };
  const expected2 = { ...expected1, activityId: '2', activityType: 'lookup', isLocal: true };
  t.deepEqual(codec.contexts.get('encode|in-1'), expected1);
  t.deepEqual(codec.contexts.get('encode|in-2'), expected2);

  // Resolve in the reverse order of scheduling; each result keeps its own activity's names.
  await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveActivity: { seq: 2, result: { completed: { result: payload('out-2') } } } }],
  });
  await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      {
        resolveActivity: {
          seq: 1,
          result: { failed: { failure: failureWithDetail('fail-1') } },
        },
      },
    ],
  });
  t.deepEqual(codec.contexts.get('decode|out-2'), expected2);
  t.deepEqual(codec.contexts.get('decode|fail-1'), expected1);
});

test('child workflow contexts use the child workflow type, never the parent type', async (t) => {
  const codec = new RecordingPayloadCodec();
  const runner = new WorkflowCodecRunner([codec], parentContext);

  await runner.encodeCompletion({
    successful: {
      commands: [
        {
          startChildWorkflowExecution: {
            seq: 1,
            workflowId: 'child-1',
            workflowType: 'ChildWorkflow',
            input: [payload('child-in')],
          },
        },
        { startChildWorkflowExecution: { seq: 2, workflowId: 'child-2', input: [payload('untyped-child-in')] } },
      ],
    },
  });
  const childContext = { type: 'workflow', namespace: 'default', workflowId: 'child-1', workflowType: 'ChildWorkflow' };
  t.deepEqual(codec.contexts.get('encode|child-in'), childContext);
  t.is((codec.contexts.get('encode|untyped-child-in') as any).workflowType, undefined);

  await runner.decodeActivation({
    runId: 'run-1',
    jobs: [{ resolveChildWorkflowExecution: { seq: 1, result: { completed: { result: payload('child-out') } } } }],
  });
  t.deepEqual(codec.contexts.get('decode|child-out'), childContext);
});

test('external signal and cancel contexts leave the unknown target workflow type undefined', async (t) => {
  const codec = new RecordingPayloadCodec();
  const runner = new WorkflowCodecRunner([codec], parentContext);

  await runner.encodeCompletion({
    successful: {
      commands: [
        {
          signalExternalWorkflowExecution: {
            seq: 1,
            workflowExecution: { namespace: 'default', workflowId: 'other' },
            signalName: 's',
            args: [payload('external-signal')],
          },
        },
        { signalExternalWorkflowExecution: { seq: 2, childWorkflowId: 'child-1', args: [payload('child-signal')] } },
        { requestCancelExternalWorkflowExecution: { seq: 3, workflowExecution: { workflowId: 'other' } } },
      ],
    },
  });
  t.deepEqual(codec.contexts.get('encode|external-signal'), {
    type: 'workflow',
    namespace: 'default',
    workflowId: 'other',
  });
  t.is((codec.contexts.get('encode|child-signal') as any).workflowType, undefined);

  await runner.decodeActivation({
    runId: 'run-1',
    jobs: [
      { resolveSignalExternalWorkflow: { seq: 1, failure: failureWithDetail('signal-failure') } },
      { resolveRequestCancelExternalWorkflow: { seq: 3, failure: failureWithDetail('cancel-failure') } },
    ],
  });
  t.is((codec.contexts.get('decode|signal-failure') as any).workflowType, undefined);
  t.is((codec.contexts.get('decode|cancel-failure') as any).workflowId, 'other');
  t.is((codec.contexts.get('decode|cancel-failure') as any).workflowType, undefined);
});

test('continue-as-new payloads use the next run workflow type', async (t) => {
  const codec = new RecordingPayloadCodec();
  const runner = new WorkflowCodecRunner([codec], parentContext);

  await runner.encodeCompletion({
    successful: {
      commands: [
        {
          continueAsNewWorkflowExecution: {
            workflowType: 'NextWorkflow',
            arguments: [payload('can-arg')],
            memo: { m: payload('can-memo') },
          },
        },
      ],
    },
  });
  t.deepEqual(codec.contexts.get('encode|can-arg'), { ...parentContext, workflowType: 'NextWorkflow' });
  t.deepEqual(codec.contexts.get('encode|can-memo'), { ...parentContext, workflowType: 'NextWorkflow' });

  const sameTypeCodec = new RecordingPayloadCodec();
  await new WorkflowCodecRunner([sameTypeCodec], parentContext).encodeCompletion({
    successful: { commands: [{ continueAsNewWorkflowExecution: { arguments: [payload('can-same')] } }] },
  });
  t.deepEqual(sameTypeCodec.contexts.get('encode|can-same'), parentContext);
});

test('activity contexts with the same seq in different workflows do not leak names', async (t) => {
  const codecA = new RecordingPayloadCodec();
  const codecB = new RecordingPayloadCodec();
  const runnerA = new WorkflowCodecRunner([codecA], { ...parentContext, workflowId: 'wf-a', workflowType: 'A' });
  const runnerB = new WorkflowCodecRunner([codecB], { ...parentContext, workflowId: 'wf-b', workflowType: 'B' });
  for (const [runner, type] of [
    [runnerA, 'actA'],
    [runnerB, 'actB'],
  ] as const) {
    await runner.encodeCompletion({
      successful: { commands: [{ scheduleActivity: { seq: 1, activityId: '1', activityType: type } }] },
    });
  }
  await runnerB.decodeActivation({
    runId: 'run-b',
    jobs: [{ resolveActivity: { seq: 1, result: { completed: { result: payload('out') } } } }],
  });
  await runnerA.decodeActivation({
    runId: 'run-a',
    jobs: [{ resolveActivity: { seq: 1, result: { completed: { result: payload('out') } } } }],
  });
  t.like(codecA.contexts.get('decode|out'), { workflowType: 'A', activityType: 'actA' });
  t.like(codecB.contexts.get('decode|out'), { workflowType: 'B', activityType: 'actB' });
});

test('contexts without type names still decode, matching the previous behavior', async (t) => {
  const codec = new RecordingPayloadCodec();
  const runner = new WorkflowCodecRunner([codec], { type: 'workflow', namespace: 'default', workflowId: 'wf-1' });
  await runner.encodeCompletion({
    successful: { commands: [{ scheduleActivity: { seq: 1, activityId: '1', arguments: [payload('in')] } }] },
  });
  t.deepEqual(codec.contexts.get('encode|in'), {
    type: 'activity',
    namespace: 'default',
    workflowId: 'wf-1',
    workflowType: undefined,
    activityId: '1',
    activityType: undefined,
    isLocal: false,
  });
});
