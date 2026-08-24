"use client";

import Link from "next/link";
import { useEffect, useState } from "react";
import {
  InstrumentSummary,
  WatchlistItem,
  addToWatchlist,
  fetchInstruments,
  fetchWatchlist,
  removeFromWatchlist,
} from "@/lib/api";

export default function WatchlistPage() {
  const [watchlist, setWatchlist] = useState<WatchlistItem[]>([]);
  const [instruments, setInstruments] = useState<InstrumentSummary[]>([]);
  const [query, setQuery] = useState("");
  const [error, setError] = useState<string | null>(null);

  async function reload() {
    try {
      const [wl, inst] = await Promise.all([fetchWatchlist(), fetchInstruments()]);
      setWatchlist(wl);
      setInstruments(inst);
      setError(null);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  }

  useEffect(() => {
    reload();
  }, []);

  const watchedIds = new Set(watchlist.map((w) => w.instrument_id));
  const matches = query
    ? instruments.filter(
        (i) =>
          !watchedIds.has(i.instrument_id) &&
          i.symbol.toLowerCase().includes(query.toLowerCase())
      )
    : [];

  async function handleAdd(instrumentId: number) {
    await addToWatchlist(instrumentId);
    setQuery("");
    await reload();
  }

  async function handleRemove(instrumentId: number) {
    await removeFromWatchlist(instrumentId);
    await reload();
  }

  return (
    <main className="p-8 max-w-3xl mx-auto">
      <h1 className="text-2xl font-bold mb-4">Watchlist</h1>
      {error && <p className="text-red-600 mb-4">{error}</p>}

      <div className="mb-6 relative">
        <input
          className="border rounded px-3 py-2 w-full"
          placeholder="Add a symbol..."
          value={query}
          onChange={(e) => setQuery(e.target.value)}
        />
        {matches.length > 0 && (
          <ul className="absolute z-10 bg-white border rounded w-full mt-1 max-h-48 overflow-auto">
            {matches.slice(0, 10).map((m) => (
              <li key={m.instrument_id}>
                <button
                  className="w-full text-left px-3 py-2 hover:bg-gray-100"
                  onClick={() => handleAdd(m.instrument_id)}
                >
                  {m.symbol} <span className="text-gray-400 text-sm">{m.asset_class}</span>
                </button>
              </li>
            ))}
          </ul>
        )}
      </div>

      <table className="w-full border-collapse">
        <thead>
          <tr className="text-left border-b">
            <th className="py-2">Symbol</th>
            <th>Asset</th>
            <th>Last price</th>
            <th></th>
          </tr>
        </thead>
        <tbody>
          {watchlist.map((row) => (
            <tr key={row.instrument_id} id={`watchlist-row-${row.instrument_id}`} className="border-b">
              <td className="py-2">
                <Link href={`/instrument/${row.instrument_id}`}>{row.symbol}</Link>
              </td>
              <td>{row.asset_class}</td>
              <td id={`price-${row.instrument_id}`}>
                {row.last_price !== null ? row.last_price : "--"}
              </td>
              <td>
                <button className="text-red-600" onClick={() => handleRemove(row.instrument_id)}>
                  remove
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </main>
  );
}
