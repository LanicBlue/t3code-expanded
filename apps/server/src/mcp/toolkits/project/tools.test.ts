import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { Tool } from "effect/unstable/ai";

import { ProjectWorkVisitSubmitResult } from "@t3tools/contracts";

import {
  ProjectDocDeleteInput,
  ProjectDocEditInput,
  ProjectDocReadInput,
  ProjectDocWriteInput,
  ProjectOperationGetInput,
  ProjectWorkGetInput,
  ProjectWorkListInput,
  ProjectWorkSubmitInput,
  ProjectWorkToolkit,
} from "./tools.ts";

const schemaHasDescription = (schema: unknown): boolean => {
  if (!schema || typeof schema !== "object") return false;
  const record = schema as Record<string, unknown>;
  if (typeof record.description === "string" && record.description.length > 0) return true;
  return [record.anyOf, record.oneOf, record.allOf]
    .filter(Array.isArray)
    .some((members) => members.some(schemaHasDescription));
};

/**
 * Identity fields the agent must never be able to state or override: the
 * server derives every one of them from the trusted session + settings.
 * (`operationId` is a business handle the agent legitimately quotes back.)
 */
const FORBIDDEN_IDENTITY_FIELDS = [
  "agentId",
  "logicalAgentId",
  "projectId",
  "projectServiceProjectId",
  "clientId",
  "consumerId",
  "executorRef",
  "providerInstanceId",
  "threadId",
  "environmentId",
  "sessionId",
  "providerSessionId",
  "projectGeneration",
  "idempotencyKey",
  "credential",
  "baseUrl",
  "keyIdHint",
];

it("exports provider-compatible object schemas with described business-only parameters", () => {
  for (const tool of Object.values(ProjectWorkToolkit.tools)) {
    const schema = Tool.getJsonSchema(tool) as {
      readonly type?: unknown;
      readonly properties?: Readonly<Record<string, unknown>>;
      readonly anyOf?: unknown;
      readonly oneOf?: unknown;
    };
    expect(
      tool.description?.length ?? 0,
      `${tool.name} should have a useful description`,
    ).toBeGreaterThan(40);
    // The no-argument list tool legitimately exports an empty object schema.
    if (tool.name !== "project_work_list") {
      expect(schema.type, `${tool.name} must export a top-level object schema`).toBe("object");
      expect(schema.anyOf, `${tool.name} must not export a root anyOf`).toBeUndefined();
      expect(schema.oneOf, `${tool.name} must not export a root oneOf`).toBeUndefined();
    }
    for (const [field, fieldSchema] of Object.entries(schema.properties ?? {})) {
      expect(
        schemaHasDescription(fieldSchema),
        `${tool.name}.${field} should explain the data`,
      ).toBe(true);
    }
  }
});

it("never accepts an identity or credential field in any tool input", () => {
  const inputs = {
    project_work_list: ProjectWorkListInput,
    project_work_get: ProjectWorkGetInput,
    project_work_submit: ProjectWorkSubmitInput,
    project_operation_get: ProjectOperationGetInput,
    project_doc_read: ProjectDocReadInput,
    project_doc_write: ProjectDocWriteInput,
    project_doc_edit: ProjectDocEditInput,
    project_doc_delete: ProjectDocDeleteInput,
  } as const;
  for (const [name, input] of Object.entries(inputs)) {
    const schema = Tool.getJsonSchemaFromSchema(input) as {
      readonly properties?: Readonly<Record<string, unknown>>;
    };
    for (const forbidden of FORBIDDEN_IDENTITY_FIELDS) {
      expect(
        Object.keys(schema.properties ?? {}),
        `${name} must not accept ${forbidden}`,
      ).not.toContain(forbidden);
    }
  }
});

it("accepts the visit-population submit result vocabulary (outcome/nextNode/reason/feedback)", () => {
  // The pinned §6.1 submit result for a visit run: outcome is required and
  // picks from task.action.outcomes; nextNode is the agent's routing choice
  // (candidates are a hint); reason/feedback/documentReceiptIds are optional
  // riders — PS owns the off-contract reason REQUIREMENT, the tool boundary
  // only carries the vocabulary.
  const decode = Schema.decodeUnknownSync(ProjectWorkSubmitInput);
  const decoded = decode({
    runId: "run_v1",
    runRevision: "run:4",
    assignmentRevision: "position:2",
    result: {
      outcome: "implementation-defect",
      nextNode: "implement",
      reason: "validation found the edge case unsatisfied",
      feedback: "re-run the auth suite after the fix",
      documentReceiptIds: ["rcpt_1"],
    },
  });
  expect(decoded.result).toEqual({
    outcome: "implementation-defect",
    nextNode: "implement",
    reason: "validation found the edge case unsatisfied",
    feedback: "re-run the auth suite after the fix",
    documentReceiptIds: ["rcpt_1"],
  });
  // The minimal visit result: outcome alone (single-continuation or terminal).
  const minimal = decode({
    runId: "run_v1",
    runRevision: "run:4",
    assignmentRevision: "position:2",
    result: { outcome: "implementation-ready" },
  });
  expect(minimal.result).toEqual({ outcome: "implementation-ready" });
  // The typed visit vocabulary pins outcome as required…
  expect(() =>
    Schema.decodeUnknownSync(ProjectWorkVisitSubmitResult)({ nextNode: "implement" }),
  ).toThrow();
  // …and carries the same fields the tool description teaches.
  expect(
    Schema.decodeUnknownSync(ProjectWorkVisitSubmitResult)({
      outcome: "design-clarification",
      nextNode: "design",
      reason: "the API contract is ambiguous about pagination",
    }),
  ).toEqual({
    outcome: "design-clarification",
    nextNode: "design",
    reason: "the API contract is ambiguous about pagination",
  });
});

it("the submit tool description teaches the visit contract: candidates hint, off-contract reason", () => {
  const description = ProjectWorkToolkit.tools.project_work_submit.description ?? "";
  // The visit population's submit semantics are agent-facing contract text:
  // they must name the outcome domain, the candidates' hint-not-constraint
  // status, and the off-contract reason requirement. The action field is
  // run-level (the wire carries it beside task, not inside it).
  expect(description).toContain("VISIT work");
  expect(description).toContain("action.outcomes");
  expect(description).toContain("action.candidates");
  expect(description).toContain("off-contract");
  expect(description).toContain('"reason"');
});

it("the flow-line surfaces are gone: no spawn tool, no flow-era submit kinds", () => {
  // work-mission-v5 Phase 7c: the consumer spawn face and the flow-era
  // state/gate/terminal action kinds died with the flow line — the toolkit
  // carries neither the tool nor the teaching.
  expect(Object.keys(ProjectWorkToolkit.tools)).not.toContain("project_flow_start");
  const description = ProjectWorkToolkit.tools.project_work_submit.description ?? "";
  for (const dead of ['"kind":"after"', '"kind":"before"', '"kind":"terminal"', "transitionId"]) {
    expect(description).not.toContain(dead);
  }
});

it("strips identity keys an agent smuggles into business arguments", () => {
  const decode = Schema.decodeUnknownSync(ProjectWorkSubmitInput);
  const decoded = decode({
    runId: "run_1",
    runRevision: "run:3",
    assignmentRevision: "position:5",
    result: { kind: "standalone", output: "done" },
    agentId: "ag_spoofed",
    projectId: "proj_spoofed",
    executorRef: "client-1:ag_somebody-else",
  });
  expect(decoded).toEqual({
    runId: "run_1",
    runRevision: "run:3",
    assignmentRevision: "position:5",
    result: { kind: "standalone", output: "done" },
  });
});
