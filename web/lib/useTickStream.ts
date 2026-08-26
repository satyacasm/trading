"use client";

import { useEffect, useRef } from "react";

export type Tick = {
  instrument_id: number;
  ts: string;
  // Wire value is a JSON string, not a number -- the Tick model on the
  // backend serializes its Decimal price field as e.g. "65000.50" over
  // /ws. Convert with Number(tick.price) at the point of use -- never
  // assume it's already numeric.
  price: string;
  quantity: string;
  side?: string | null;
};

const WS_URL =
  (process.env.NEXT_PUBLIC_API_URL ?? "http://localhost:8000").replace(/^http/, "ws") + "/ws";
const MAX_BACKOFF_MS = 10_000;

// NOTE: callers must pass a MEMOIZED `instrumentIds` array (e.g. via
// useMemo, or a stable reference from state). The subscription-diffing
// effect below keys off array identity, so a fresh array literal on every
// render would re-run the diff (harmlessly, since the diff itself is a
// no-op set comparison) but is still wasted work -- memoize to avoid it.
export function useTickStream(instrumentIds: number[], onTick: (tick: Tick) => void): void {
  const wsRef = useRef<WebSocket | null>(null);
  const subscribedRef = useRef<Set<number>>(new Set());
  const onTickRef = useRef(onTick);
  const backoffRef = useRef(1000);
  const closedByUsRef = useRef(false);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const instrumentIdsRef = useRef<number[]>(instrumentIds);

  // Refs must not be written during render -- sync them from props in
  // effects instead. These run after every render so the mount-only
  // connection effect and the async ws.onopen handler always see the
  // latest callback/instrument list via the refs.
  useEffect(() => {
    onTickRef.current = onTick;
  });
  useEffect(() => {
    instrumentIdsRef.current = instrumentIds;
  });

  // Mount-only effect: owns the single WebSocket connection and its
  // reconnect-with-backoff loop. Intentionally does not depend on
  // `instrumentIds` -- the subscription-diffing effect below handles
  // changes to that array on an already-open socket.
  useEffect(() => {
    closedByUsRef.current = false;

    function connect() {
      const ws = new WebSocket(WS_URL);
      wsRef.current = ws;

      ws.onopen = () => {
        backoffRef.current = 1000;
        subscribedRef.current = new Set();
        for (const id of instrumentIdsRef.current) {
          ws.send(JSON.stringify({ action: "subscribe", instrument_id: id }));
          subscribedRef.current.add(id);
        }
      };

      ws.onmessage = (event) => {
        try {
          onTickRef.current(JSON.parse(event.data) as Tick);
        } catch (err) {
          console.warn("useTickStream: dropping malformed frame", event.data, err);
        }
      };

      ws.onclose = () => {
        if (closedByUsRef.current) return;
        // Reconnect with exponential backoff, capped at MAX_BACKOFF_MS.
        const delay = backoffRef.current;
        backoffRef.current = Math.min(delay * 2, MAX_BACKOFF_MS);
        reconnectTimerRef.current = setTimeout(connect, delay);
      };
    }

    connect();

    return () => {
      closedByUsRef.current = true;
      if (reconnectTimerRef.current !== null) {
        clearTimeout(reconnectTimerRef.current);
        reconnectTimerRef.current = null;
      }
      wsRef.current?.close();
    };
    // Mount-only: connection lifecycle is intentionally independent of
    // instrumentIds/onTick, which are read via refs kept current above.
  }, []);

  // Diffs the desired subscription set against what's already subscribed
  // on the live socket, sending only the incremental subscribe/unsubscribe
  // messages needed. Requires a memoized `instrumentIds` (see note above).
  useEffect(() => {
    const ws = wsRef.current;
    if (!ws || ws.readyState !== WebSocket.OPEN) return;
    const next = new Set(instrumentIds);
    for (const id of next) {
      if (!subscribedRef.current.has(id)) {
        ws.send(JSON.stringify({ action: "subscribe", instrument_id: id }));
      }
    }
    for (const id of subscribedRef.current) {
      if (!next.has(id)) {
        ws.send(JSON.stringify({ action: "unsubscribe", instrument_id: id }));
      }
    }
    subscribedRef.current = next;
  }, [instrumentIds]);
}
