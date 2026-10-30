import type { PipelineStepDef } from './types';

/** One output a pipeline step can publish to its children. */
export interface StepOutputDescriptor {
  name: string;
  type: 'table' | 'scalar';
  /** True when the engine supplies the output rather than the step declaring it. */
  derived: boolean;
}

/** A direct parent and the outputs it can publish to this step. */
export interface ParentStepOutput {
  stepName: string;
  outputs: StepOutputDescriptor[];
}

/**
 * The engine's step reference is the step name with every run of non-alphanumeric
 * characters replaced by ``_`` and the outer underscores removed. Keep this in lockstep
 * with backend ``slug_name`` so the tokens shown in the UI are the ones the engine accepts.
 */
export function stepSlug(value: string): string {
  return value.replace(/[^A-Za-z0-9]+/g, '_').replace(/^_+|_+$/g, '');
}

export function outputToken(stepName: string, output: string): string {
  return `{{steps.${stepSlug(stepName)}.${output}}}`;
}

/**
 * Every output a step may expose to a child.
 *
 * Mirrors ``pipeline_engine.declared_outputs``: explicit declarations first, then the
 * engine-derived outputs. De-duplicated so a declared ``table`` cannot appear twice.
 */
export function deriveStepOutputs(step: PipelineStepDef): StepOutputDescriptor[] {
  const args = step.args ?? {};
  const declared = args.outputs;
  const values: StepOutputDescriptor[] = [];
  const seen = new Set<string>();
  const add = (name: string, type: 'table' | 'scalar', derived: boolean) => {
    if (!name || seen.has(name)) return;
    seen.add(name);
    values.push({ name, type, derived });
  };

  if (declared && typeof declared === 'object' && !Array.isArray(declared)) {
    Object.entries(declared as Record<string, unknown>).forEach(([name, spec]) => {
      const kind =
        spec === 'table' ||
        (spec &&
          typeof spec === 'object' &&
          (spec as { type?: unknown }).type === 'table')
          ? 'table'
          : 'scalar';
      add(name, kind, false);
    });
  }

  const materialize = args.materialize;
  if (materialize && typeof materialize === 'object') add('table', 'table', true);
  if (step.step_type === 'raw_sql') add('rows', 'scalar', true);
  if (step.step_type === 'ingest') {
    add('table', 'table', true);
    add('rows', 'scalar', true);
  }
  if (step.step_type === 'dbt_run' || step.step_type === 'dbt_test') {
    add('dbt_summary', 'scalar', true);
  }
  if (step.step_type === 'pipeline') {
    add('run_id', 'scalar', true);
    add('status', 'scalar', true);
  }
  return values;
}
