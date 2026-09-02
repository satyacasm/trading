"use client";

import { useCallback, useEffect, useState } from "react";
import { fetchPortfolios, type Portfolio } from "@/lib/api";

const STORAGE_KEY = "trading.selectedPortfolioId";

/**
 * The portfolio list plus which one is selected, shared by the order
 * ticket and the portfolio page.
 *
 * The selection is persisted in localStorage so the two surfaces agree:
 * picking a portfolio on the chart page and then opening /portfolio shows
 * the same account, rather than silently reverting to the first one and
 * displaying a different set of positions than you just traded into.
 *
 * A stored id that no longer resolves (the portfolio was deleted, or the
 * database was reset) falls back to the first available portfolio rather
 * than leaving the ticket pointed at nothing.
 */
export function usePortfolios(): {
  portfolios: Portfolio[];
  selected: Portfolio | null;
  selectedId: number | null;
  setSelectedId: (id: number) => void;
  error: string | null;
  reload: () => Promise<void>;
} {
  const [portfolios, setPortfolios] = useState<Portfolio[]>([]);
  const [selectedId, setSelectedIdState] = useState<number | null>(null);
  const [error, setError] = useState<string | null>(null);

  const reload = useCallback(async () => {
    try {
      const list = await fetchPortfolios();
      setPortfolios(list);
      setError(null);
      setSelectedIdState((current) => {
        if (current !== null && list.some((p) => p.portfolio_id === current)) return current;
        // localStorage can throw (private mode, disabled site data), and can
        // hold an id from a database that has since been reset.
        let stored: number | null = null;
        try {
          const raw = window.localStorage.getItem(STORAGE_KEY);
          stored = raw === null ? null : Number(raw);
        } catch {
          stored = null;
        }
        if (stored !== null && list.some((p) => p.portfolio_id === stored)) return stored;
        return list.length > 0 ? list[0].portfolio_id : null;
      });
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }, []);

  useEffect(() => {
    // eslint-disable-next-line react-hooks/set-state-in-effect
    reload();
  }, [reload]);

  const setSelectedId = useCallback((id: number) => {
    setSelectedIdState(id);
    try {
      window.localStorage.setItem(STORAGE_KEY, String(id));
    } catch {
      // Selection still works for this session; it just will not persist.
    }
  }, []);

  const selected = portfolios.find((p) => p.portfolio_id === selectedId) ?? null;

  return { portfolios, selected, selectedId, setSelectedId, error, reload };
}
