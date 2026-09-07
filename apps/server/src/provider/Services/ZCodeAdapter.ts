/**
 * ZCodeAdapter — shape type for the ZCode provider adapter.
 *
 * The driver model ({@link ../Drivers/ZCodeDriver}) bundles one adapter per
 * instance as a captured closure, so this module only retains the shape
 * interface as a naming anchor for the driver bundle.
 *
 * @module ZCodeAdapter
 */
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "./ProviderAdapter.ts";

/**
 * ZCodeAdapterShape — per-instance ZCode adapter contract.
 */
export interface ZCodeAdapterShape extends ProviderAdapterShape<ProviderAdapterError> {}
