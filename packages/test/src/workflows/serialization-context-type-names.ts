/**
 * Workflows for test-serialization-context-type-names, bundled with the context-recording payload
 * converter so each command's payload records the serialization context the sandbox used.
 */
import * as wf from '@temporalio/workflow';

const { charge } = wf.proxyActivities<{ charge(input: string): Promise<unknown> }>({
  startToCloseTimeout: '1 minute',
});
const { lookup } = wf.proxyLocalActivities<{ lookup(input: string): Promise<unknown> }>({
  startToCloseTimeout: '1 minute',
});

export async function typeNamesChild(_input: string): Promise<void> {}

export async function typeNamesParent(): Promise<unknown[]> {
  const external = wf.getExternalWorkflowHandle('external-id');
  return await Promise.all([
    charge('activity-input'),
    lookup('local-activity-input'),
    wf.executeChild(typeNamesChild, { workflowId: 'child-id', args: ['child-input'] }),
    external.signal('someSignal', 'external-signal-input'),
  ]);
}

export async function typeNamesContinueAsNew(): Promise<void> {
  await wf.makeContinueAsNewFunc<typeof typeNamesChild>({ workflowType: 'typeNamesChild' })('can-input');
}
