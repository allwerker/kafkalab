import { useEffect, useState } from "react";
import { Eye, Lightbulb, Target, Trophy, XCircle } from "lucide-react";
import type { Difficulty, Drill, LeaderboardEntry } from "../api/types";
import { loadPref, savePref } from "../lib/browser";
import { fmtAge, fmtDuration } from "../lib/format";
import { useBoard } from "../store/store";
import { toast, toastError } from "../store/toasts";
import { useApi, useNow } from "./context";
import { Empty } from "./ui";

const DIFFICULTIES: { id: Difficulty; label: string; blurb: string }[] = [
  { id: "easy", label: "Easy", blurb: "Loud symptoms, one service" },
  { id: "medium", label: "Medium", blurb: "Subtle signals, read the dashboards" },
  { id: "hard", label: "Hard", blurb: "Misleading symptoms" },
];

function Leaderboard({ entries }: { entries: LeaderboardEntry[] }) {
  if (!entries.length) return <Empty>No scores yet — be the first.</Empty>;
  return (
    <ol className="leaderboard">
      {entries.slice(0, 10).map((e, i) => (
        <li key={`${e.player}-${e.at}`}>
          <span className="rank mono">{i + 1}</span>
          <span className="player">{e.player}</span>
          <span className="muted small">{e.scenario}</span>
          <span className="mono small">{fmtDuration(e.durationSec)}</span>
          <b className="mono">{e.score}</b>
        </li>
      ))}
    </ol>
  );
}

function RunningDrill({ drill, onUpdate }: { drill: Drill; onUpdate: (d: Drill) => void }) {
  const apiClient = useApi();
  const now = useNow();
  const call = async (fn: () => Promise<Drill>) => {
    try {
      const d = await fn();
      onUpdate(d);
      if (d.status === "solved") toast("success", `Solved! ${d.score} points`);
      else if (d.guesses.length && !d.guesses[d.guesses.length - 1]!.correct && d.status === "running") toast("warning", "Not quite — keep investigating");
    } catch (e) {
      toastError(e);
    }
  };
  const wrong = new Set(drill.guesses.filter((g) => !g.correct).map((g) => g.scenario));
  return (
    <div className="drill-run">
      <div className="drill-run__head">
        <span className="drill-timer mono" aria-label="elapsed time">
          {fmtAge(drill.startedAt, now)}
        </span>
        <span className="tag tag--amber">{drill.difficulty}</span>
        <span className="muted small">Something is wrong in the lab. Use Grafana, the live view and alerts — then name the scenario.</span>
      </div>
      <div className="drill-hints">
        {drill.hints.map((h, i) => (
          <p key={i} className="hint">
            <Lightbulb size={14} /> {h}
          </p>
        ))}
        <button className="btn btn--sm btn--ghost" onClick={() => call(() => apiClient.hint(drill.id))} disabled={drill.hints.length >= 3}>
          <Lightbulb size={12} /> Hint ({drill.hintsUsed}/3, −100 pts)
        </button>
      </div>
      <div className="candidates" role="group" aria-label="Guess the scenario">
        {drill.candidates.map((c) => (
          <button key={c.id} className={`candidate ${wrong.has(c.id) ? "candidate--wrong" : ""}`} disabled={wrong.has(c.id)} onClick={() => call(() => apiClient.guess(drill.id, c.id))}>
            {wrong.has(c.id) && <XCircle size={13} />} {c.name}
          </button>
        ))}
      </div>
      <button className="btn btn--sm" onClick={() => call(() => apiClient.reveal(drill.id))}>
        <Eye size={12} /> Give up &amp; reveal
      </button>
    </div>
  );
}

export function IncidentDrill() {
  const apiClient = useApi();
  const drill = useBoard((b) => b.drill);
  const setDrill = useBoard((b) => b.setDrill);
  const [player, setPlayer] = useState(() => loadPref("player", ""));
  const [difficulty, setDifficulty] = useState<Difficulty>("easy");
  const [board, setBoard] = useState<LeaderboardEntry[]>([]);
  const status = drill?.status;

  useEffect(() => {
    apiClient.leaderboard().then(setBoard, () => undefined);
  }, [apiClient, status]);

  const start = async () => {
    savePref("player", player);
    try {
      setDrill(await apiClient.startDrill(difficulty, player.trim() || "anonymous"));
      toast("warning", "Incident drill started", "A hidden fault is now active.");
    } catch (e) {
      toastError(e);
    }
  };

  return (
    <div className="drill">
      <div className="drill__main">
        {drill?.status === "running" ? (
          <RunningDrill drill={drill} onUpdate={setDrill} />
        ) : (
          <form
            className="drill-start"
            onSubmit={(e) => {
              e.preventDefault();
              void start();
            }}
          >
            {drill && (
              <div className={`drill-result drill-result--${drill.status}`} role="status">
                <Target size={16} /> Last drill <b>{drill.status}</b>
                {drill.scenario && (
                  <>
                    {" "}
                    — it was <b>{drill.candidates.find((c) => c.id === drill.scenario)?.name ?? drill.scenario}</b>
                  </>
                )}
                {drill.score !== undefined && <span className="mono"> · {drill.score} pts</span>}
              </div>
            )}
            <label className="field" htmlFor="drill-player">
              <span>Player name</span>
              <input id="drill-player" value={player} maxLength={32} placeholder="anonymous" onChange={(e) => setPlayer(e.target.value)} />
            </label>
            <div className="difficulties" role="radiogroup" aria-label="Difficulty">
              {DIFFICULTIES.map((d) => (
                <button type="button" key={d.id} role="radio" aria-checked={difficulty === d.id} className={`difficulty ${difficulty === d.id ? "is-on" : ""}`} onClick={() => setDifficulty(d.id)}>
                  <b>{d.label}</b>
                  <span>{d.blurb}</span>
                </button>
              ))}
            </div>
            <button type="submit" className="btn btn--primary">
              <Target size={14} /> Start drill
            </button>
          </form>
        )}
      </div>
      <aside className="drill__board">
        <h3 className="subhead">
          <Trophy size={14} /> Leaderboard
        </h3>
        <Leaderboard entries={board} />
      </aside>
    </div>
  );
}
