/**
 * Checks the workflow and activity type names the workflow sandbox puts in serialization contexts,
 * without a Temporal server: activations are fed straight to a VM workflow whose bundled payload
 * converter records each context it sees.
 */
import test from 'ava';
import Long from 'long';
import type { Payload } from '@temporalio/common';
import { defaultPayloadConverter, TypedSearchAttributes } from '@temporalio/common';
import { encode } from '@temporalio/common/encoding';
import { coresdk } from '@temporalio/proto';
import type { WorkflowInfo } from '@temporalio/workflow';
import { bundleWorkflowCode } from '@temporalio/worker';
import { parseWorkflowCode, VMWorkflowCreator } from '@temporalio/worker/internal';
import { createUnsafeRandomSource } from '@temporalio/workflow/internal';
import { ECHO_CONTEXT_KEY, recordedContext } from './payload-converters/context-recording-payload-converter';

const parentContext = {
  type: 'workflow',
  namespace: 'default',
  workflowId: 'parent-id',
  workflowType: 'typeNamesParent',
};

let creator: VMWorkflowCreator;

test.before(async () => {
  const { code } = await bundleWorkflowCode({
    workflowsPath: require.resolve('./workflows/serialization-context-type-names'),
    payloadConverterPath: require.resolve('./payload-converters/context-recording-payload-converter'),
    logger: { trace() {}, debug() {}, info() {}, warn() {}, error() {}, log() {} },
  });
  creator = await VMWorkflowCreator.create(parseWorkflowCode(code), 400, new Set(['lookup']));
});

test.after.always(async () => {
  await creator?.destroy();
});

function workflowInfo(workflowType: string, runId: string): WorkflowInfo {
  return {
    workflowType,
    runId,
    workflowId: 'parent-id',
    namespace: 'default',
    firstExecutionRunId: runId,
    originalExecutionRunId: runId,
    attempt: 1,
    taskTimeoutMs: 1000,
    taskQueue: 'test',
    searchAttributes: {},
    typedSearchAttributes: new TypedSearchAttributes(),
    historyLength: 3,
    historySize: 300,
    continueAsNewSuggested: false,
    targetWorkerDeploymentVersionChanged: false,
    unsafe: {
      isReplaying: false,
      isReplayingHistoryEvents: false,
      now: Date.now,
      random: createUnsafeRandomSource(Math.random),
    },
    startTime: new Date(),
    runStartTime: new Date(),
  };
}

async function startWorkflow(workflowType: string) {
  const runId = `${workflowType}-run`;
  const workflow = await creator.createWorkflow({
    info: workflowInfo(workflowType, runId),
    randomnessSeed: Long.fromInt(1337).toBytes(),
    now: Date.now(),
    showStackTraceSources: true,
  });
  const activate = async (jobs: coresdk.workflow_activation.IWorkflowActivationJob[]) =>
    workflow.activate(
      coresdk.workflow_activation.WorkflowActivation.fromObject({ runId, timestamp: { seconds: 1 }, jobs })
    );
  const first = await activate([
    {
      initializeWorkflow: {
        workflowId: 'parent-id',
        workflowType,
        randomnessSeed: Long.fromInt(1337),
        firstExecutionRunId: runId,
        originalExecutionRunId: runId,
        attempt: 1,
      },
    },
  ]);
  return { workflow, activate, commands: first.successful?.commands ?? [] };
}

/** A payload whose decoding returns the context the sandbox decoded it with. */
function echoContextPayload(): Payload {
  const payload = defaultPayloadConverter.toPayload(null);
  return { ...payload, metadata: { ...payload.metadata, [ECHO_CONTEXT_KEY]: encode('1') } };
}

test('workflow sandbox puts owner type names in activity, child, and external contexts', async (t) => {
  const { workflow, activate, commands } = await startWorkflow('typeNamesParent');
  try {
    const find = <K extends keyof coresdk.workflow_commands.IWorkflowCommand>(key: K) =>
      commands.find((c) => c[key] != null)?.[key] as NonNullable<coresdk.workflow_commands.IWorkflowCommand[K]>;

    const activity = find('scheduleActivity');
    const activityContext = {
      type: 'activity',
      namespace: 'default',
      activityId: activity.activityId,
      activityType: 'charge',
      workflowId: 'parent-id',
      workflowType: 'typeNamesParent',
      isLocal: false,
    };
    t.deepEqual(recordedContext(activity.arguments?.[0]), activityContext);

    const localActivity = find('scheduleLocalActivity');
    const localActivityContext = {
      ...activityContext,
      activityId: localActivity.activityId,
      activityType: 'lookup',
      isLocal: true,
    };
    t.deepEqual(recordedContext(localActivity.arguments?.[0]), localActivityContext);

    const childContext = { ...parentContext, workflowId: 'child-id', workflowType: 'typeNamesChild' };
    t.deepEqual(recordedContext(find('startChildWorkflowExecution').input?.[0]), childContext);

    // The external target's type is unknown; it must not be the parent's.
    const signalContext = recordedContext(find('signalExternalWorkflowExecution').args?.[0]) as Record<string, unknown>;
    t.is(signalContext.workflowId, 'external-id');
    t.is(signalContext.workflowType, undefined);

    // Results are decoded with the same contexts that encoded the inputs.
    const completion = await activate([
      {
        resolveChildWorkflowExecutionStart: { seq: find('startChildWorkflowExecution').seq, succeeded: { runId: 'r' } },
      },
      {
        resolveChildWorkflowExecution: {
          seq: find('startChildWorkflowExecution').seq,
          result: { completed: { result: echoContextPayload() } },
        },
      },
      { resolveSignalExternalWorkflow: { seq: find('signalExternalWorkflowExecution').seq } },
      { resolveActivity: { seq: activity.seq, result: { completed: { result: echoContextPayload() } } } },
      {
        resolveActivity: {
          seq: localActivity.seq,
          isLocal: true,
          result: { completed: { result: echoContextPayload() } },
        },
      },
    ]);
    const result = completion.successful?.commands?.find((c) => c.completeWorkflowExecution)?.completeWorkflowExecution
      ?.result;
    t.deepEqual(recordedContext(result), parentContext);
    t.deepEqual(defaultPayloadConverter.fromPayload(result!), [
      activityContext,
      localActivityContext,
      childContext,
      null, // signal() resolves to undefined, which JSON encodes as null
    ]);
  } finally {
    await workflow.dispose();
  }
});

test('workflow sandbox labels continue-as-new arguments with the next run workflow type', async (t) => {
  const { workflow, commands } = await startWorkflow('typeNamesContinueAsNew');
  try {
    const continueAsNew = commands.find((c) => c.continueAsNewWorkflowExecution)?.continueAsNewWorkflowExecution;
    t.is(continueAsNew?.workflowType, 'typeNamesChild');
    t.deepEqual(recordedContext(continueAsNew?.arguments?.[0]), {
      ...parentContext,
      workflowType: 'typeNamesChild',
    });
  } finally {
    await workflow.dispose();
  }
});
