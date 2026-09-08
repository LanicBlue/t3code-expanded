import { describe, expect, it } from "@effect/vitest";

import { zcodeModelCatalogToServerModels } from "./ZCodeProvider.ts";
import type { ZcodeModelDescriptor, ZcodeModelRef } from "../zcode/ZcodeProtocolClient.ts";

const ref = (providerId: string, modelId: string): ZcodeModelRef => ({ providerId, modelId });
const descriptor = (modelId: string): ZcodeModelDescriptor => ({
  ref: ref("builtin:bigmodel-coding-plan", modelId),
  label: modelId,
});

describe("zcodeModelCatalogToServerModels thoughtLevel capabilities", () => {
  it("exposes the reasoning selector with default and current level", () => {
    const models = zcodeModelCatalogToServerModels(
      [descriptor("GLM-5.3")],
      ref("builtin:bigmodel-coding-plan", "GLM-5.3"),
      [],
      {
        available: [
          { label: "low", value: "low" },
          { label: "high", value: "high" },
          { label: "max", value: "max" },
        ],
        current: "max",
        defaultLevel: "max",
        enabled: true,
      },
    );

    expect(models).toHaveLength(1);
    const capabilities = models[0]!.capabilities;
    expect(capabilities).not.toBeNull();
    const select = capabilities?.optionDescriptors?.find(
      (option): option is Extract<typeof option, { type: "select" }> => option.type === "select",
    );
    expect(select?.id).toBe("reasoningEffort");
    expect(select?.label).toBe("Reasoning");
    expect(select?.options.map((option) => option.id)).toEqual(["low", "high", "max"]);
    // defaultLevel marks the default option; current is re-stated per turn.
    expect(select?.options.find((option) => option.isDefault)?.id).toBe("max");
    expect(select?.currentValue).toBe("max");
  });

  it("falls back to the first level when current is outside the catalog", () => {
    const models = zcodeModelCatalogToServerModels([descriptor("GLM-5.3")], undefined, [], {
      available: [
        { label: "low", value: "low" },
        { label: "high", value: "high" },
      ],
      current: "ultra",
    });
    const select = models[0]!.capabilities?.optionDescriptors?.find(
      (option): option is Extract<typeof option, { type: "select" }> => option.type === "select",
    );
    expect(select?.currentValue).toBe("low");
  });

  it("keeps capabilities null without a level catalog, and null on custom models", () => {
    const models = zcodeModelCatalogToServerModels(
      [descriptor("GLM-5.3")],
      undefined,
      ["custom/experimental"],
      undefined,
    );
    expect(models[0]!.capabilities).toBeNull();
    expect(models[1]!.isCustom).toBe(true);
    expect(models[1]!.capabilities).toBeNull();
  });
});
