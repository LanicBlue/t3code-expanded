import { expect, it } from "@effect/vitest";
import * as Schema from "effect/Schema";
import { Tool } from "effect/unstable/ai";

import {
  ProjectDocDeleteInput,
  ProjectDocEditInput,
  ProjectDocReadInput,
  ProjectDocWriteInput,
  ProjectFlowStartInput,
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
    project_flow_start: ProjectFlowStartInput,
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
