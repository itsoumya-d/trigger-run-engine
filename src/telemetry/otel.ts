/**
 * Minimal OpenTelemetry-compatible W3C traceparent + span recorder + event loop delay meter.
 */
import { monitorEventLoopDelay } from "node:perf_hooks";

export interface RecordedSpan {
  traceId: string;
  spanId: string;
  parentSpanId?: string;
  name: string;
  startMs: number;
  endMs?: number;
  attributes: Record<string, string | number | boolean>;
  status: "UNSET" | "OK" | "ERROR";
}

export function generateHexId(bytes: number, seedCounter: number): string {
  const hex = seedCounter.toString(16).padStart(bytes * 2, "0");
  return hex.slice(-bytes * 2);
}

export function formatTraceparent(traceId: string, spanId: string): string {
  return `00-${traceId}-${spanId}-01`;
}

export function parseTraceparent(header: string): { traceId: string; parentSpanId: string } | null {
  const match = /^00-([0-9a-f]{32})-([0-9a-f]{16})-[0-9a-f]{2}$/.exec(header.trim());
  if (!match || !match[1] || !match[2]) return null;
  return { traceId: match[1], parentSpanId: match[2] };
}

export class OTelTracer {
  private counter = 1;
  readonly spans: RecordedSpan[] = [];

  startRootSpan(name: string, startMs: number, attrs: Record<string, string | number | boolean> = {}): RecordedSpan {
    const traceId = generateHexId(16, this.counter++);
    const spanId = generateHexId(8, this.counter++);
    const span: RecordedSpan = { traceId, spanId, name, startMs, attributes: attrs, status: "UNSET" };
    this.spans.push(span);
    return span;
  }

  startChildSpan(
    name: string,
    traceparent: string,
    startMs: number,
    attrs: Record<string, string | number | boolean> = {},
  ): RecordedSpan {
    const parsed = parseTraceparent(traceparent);
    const traceId = parsed ? parsed.traceId : generateHexId(16, this.counter++);
    const spanId = generateHexId(8, this.counter++);
    const span: RecordedSpan = {
      traceId,
      spanId,
      parentSpanId: parsed?.parentSpanId,
      name,
      startMs,
      attributes: attrs,
      status: "UNSET",
    };
    this.spans.push(span);
    return span;
  }

  finishSpan(span: RecordedSpan, endMs: number, status: "OK" | "ERROR", errorMsg?: string): void {
    span.endMs = endMs;
    span.status = status;
    if (errorMsg) span.attributes["exception.message"] = errorMsg;
  }
}

export function sampleEventLoopP95Ms(): number {
  const h = monitorEventLoopDelay({ resolution: 10 });
  h.enable();
  h.disable();
  return Number((h.percentile(95) / 1e6).toFixed(3));
}
