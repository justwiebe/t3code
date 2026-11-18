// The UI Claude Code mods draw for each Claude thread. Headless Claude pushes `ui_*` system
// messages and draws the band and panes only when asked with `ui_render`; the protocol is
// internal to the Claude CLI, so a failure here is logged and never affects a turn.
import {
  CLAUDE_PLUGIN_UI_BAND_COLUMNS,
  type ClaudePluginUiClosePaneInput,
  type ClaudePluginUiElement,
  type ClaudePluginUiPane,
  type ClaudePluginUiPressInput,
  type ClaudePluginUiSnapshot,
  EMPTY_CLAUDE_PLUGIN_UI_SNAPSHOT,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";

class ClaudePluginUiRequestError extends Schema.TaggedError<ClaudePluginUiRequestError>()(
  "ClaudePluginUiRequestError",
  { cause: Schema.Defect() },
) {}

/** Sends one control request on the thread's live Claude query. */
export type ClaudeControlRequest = (request: Record<string, unknown>) => Promise<unknown>;

const BAND_INSTANCE_ID = "above-prompt";
const BAND_MAX_ROWS = 12;
const PANE_MAX_ROWS = 40;
const PANE_DEFAULT_COLUMNS = 44;

interface ThreadEntry {
  request: ClaudeControlRequest | undefined;
  snapshot: ClaudePluginUiSnapshot;
  rendering: boolean;
  renderAgain: boolean;
}

export interface ClaudePluginUiShape {
  readonly attach: (threadId: ThreadId, request: ClaudeControlRequest) => Effect.Effect<void>;
  readonly detach: (threadId: ThreadId, request: ClaudeControlRequest) => Effect.Effect<void>;
  /** `from` is the sending query's control-request sender; messages from a replaced query are dropped. */
  readonly ingest: (
    threadId: ThreadId,
    message: unknown,
    from: ClaudeControlRequest | undefined,
  ) => Effect.Effect<void>;
  readonly subscribe: (threadId: ThreadId) => Stream.Stream<ClaudePluginUiSnapshot>;
  readonly press: (input: ClaudePluginUiPressInput) => Effect.Effect<void>;
  readonly closePane: (input: ClaudePluginUiClosePaneInput) => Effect.Effect<void>;
}

export class ClaudePluginUi extends Context.Service<ClaudePluginUi, ClaudePluginUiShape>()(
  "t3/provider/ClaudePluginUi",
) {}

/** A plain object, not null or an array. */
const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);

/** The SDK resolves a control request with the whole `{ subtype, request_id, response }` envelope. */
const responseOf = (envelope: unknown): unknown =>
  isRecord(envelope) && "response" in envelope ? envelope.response : envelope;

/** A tree worth drawing: an element the plugins drew, not the engine's own placeholder. */
export const treeFromRenderResponse = (envelope: unknown): ClaudePluginUiElement | null => {
  const response = responseOf(envelope);
  if (!isRecord(response) || response.hooked !== true) return null;
  const tree = response.tree;
  if (!isRecord(tree) || typeof tree.type !== "string" || tree.type === "engine") return null;
  return tree as unknown as ClaudePluginUiElement;
};

/**
 * The open panes a `ui_panes` push or answer lists, keeping each pane's last
 * drawn tree until it draws again. `null` when the value is not a roster.
 */
const panesFromRoster = (
  roster: unknown,
  previous: ReadonlyArray<ClaudePluginUiPane>,
): ReadonlyArray<ClaudePluginUiPane> | null => {
  if (!isRecord(roster) || !Array.isArray(roster.panes)) return null;
  return roster.panes.flatMap((pane): ReadonlyArray<ClaudePluginUiPane> => {
    if (!isRecord(pane) || typeof pane.id !== "string" || typeof pane.plugin !== "string") {
      return [];
    }
    const id = pane.id;
    return [
      {
        id,
        plugin: pane.plugin,
        title: typeof pane.title === "string" && pane.title !== "" ? pane.title : id,
        columns:
          typeof pane.columns === "number" && pane.columns > 0
            ? pane.columns
            : PANE_DEFAULT_COLUMNS,
        tree: previous.find((existing) => existing.id === id)?.tree ?? null,
      },
    ];
  });
};

const samePanes = (a: ReadonlyArray<ClaudePluginUiPane>, b: ReadonlyArray<ClaudePluginUiPane>) =>
  a.length === b.length &&
  a.every(
    (pane, index) =>
      pane.id === b[index]?.id &&
      pane.title === b[index]?.title &&
      pane.columns === b[index]?.columns,
  );

/** Builds the service: per-thread plugin UI state, renders, and subscriber signals. */
export const make = Effect.sync(() => {
  const threads = new Map<ThreadId, ThreadEntry>();
  let toastCount = 0;
  // Each subscriber's one-slot "changed" signal. A subscriber reads the current
  // snapshot when signalled, so a slow client holds one pending signal, never a
  // backlog of snapshots.
  const watchers = new Map<ThreadId, Set<Queue.Queue<void>>>();

  /** The thread's entry, created empty on first use. */
  const entryFor = (threadId: ThreadId): ThreadEntry => {
    let entry = threads.get(threadId);
    if (entry === undefined) {
      entry = {
        request: undefined,
        snapshot: EMPTY_CLAUDE_PLUGIN_UI_SNAPSHOT,
        rendering: false,
        renderAgain: false,
      };
      threads.set(threadId, entry);
    }
    return entry;
  };

  /** Applies `next` to the thread's snapshot and signals its subscribers when it changed. */
  const update = (
    threadId: ThreadId,
    next: (snapshot: ClaudePluginUiSnapshot) => ClaudePluginUiSnapshot,
  ) =>
    Effect.suspend(() => {
      const entry = entryFor(threadId);
      const snapshot = next(entry.snapshot);
      if (snapshot === entry.snapshot) return Effect.void;
      entry.snapshot = snapshot;
      return Effect.forEach(
        watchers.get(threadId) ?? [],
        (signal) => Queue.offer(signal, undefined),
        { discard: true },
      );
    });

  /** Forgets a thread whose query has gone, once nothing is drawing for it. */
  const removeIfIdle = (threadId: ThreadId, entry: ThreadEntry) =>
    Effect.sync(() => {
      if (
        !entry.rendering &&
        entry.request === undefined &&
        entry.snapshot === EMPTY_CLAUDE_PLUGIN_UI_SNAPSHOT &&
        threads.get(threadId) === entry
      ) {
        threads.delete(threadId);
      }
    });

  /** Sends one control request, yielding its response or `undefined` once logged. */
  const send = (threadId: ThreadId, request: ClaudeControlRequest, body: Record<string, unknown>) =>
    Effect.tryPromise({
      try: () => request(body),
      catch: (cause) => new ClaudePluginUiRequestError({ cause }),
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logDebug("claude-plugin-ui.request-failed", {
          threadId,
          subtype: body.subtype,
          cause,
        }).pipe(Effect.as(undefined)),
      ),
    );

  const renderRequest = (
    component: "AbovePrompt" | "Pane",
    instanceId: string,
    columns: number,
  ) => {
    const rows = component === "Pane" ? PANE_MAX_ROWS : BAND_MAX_ROWS;
    return {
      subtype: "ui_render",
      surface: "desktop",
      component,
      instance_id: instanceId,
      props: {
        hasSurvey: false,
        isWorking: false,
        maxRows: rows,
        bodyColumns: columns,
        scroll: { offset: 0, bodyRows: rows },
      },
    };
  };

  /** Draws the band and every open pane, storing trees only while this query is current. */
  const renderAll = Effect.fn("ClaudePluginUi.renderAll")(function* (
    threadId: ThreadId,
    request: ClaudeControlRequest,
  ) {
    const isCurrent = () => threads.get(threadId)?.request === request;
    const band = treeFromRenderResponse(
      yield* send(
        threadId,
        request,
        renderRequest("AbovePrompt", BAND_INSTANCE_ID, CLAUDE_PLUGIN_UI_BAND_COLUMNS),
      ),
    );
    if (!isCurrent()) return;
    yield* update(threadId, (snapshot) => ({ ...snapshot, band }));
    const panes = entryFor(threadId).snapshot.panes;
    const trees = yield* Effect.forEach(
      panes,
      (pane) =>
        send(threadId, request, renderRequest("Pane", pane.id, pane.columns)).pipe(
          Effect.map((response) => [pane.id, treeFromRenderResponse(response)] as const),
        ),
      { concurrency: "unbounded" },
    );
    if (!isCurrent()) return;
    const treeById = new Map(trees);
    yield* update(threadId, (snapshot) => ({
      ...snapshot,
      panes: snapshot.panes.map((pane) =>
        treeById.has(pane.id) ? { ...pane, tree: treeById.get(pane.id) ?? null } : pane,
      ),
    }));
  });

  /** Draws everything, folding invalidates that land while a render is in flight into one more. */
  const requestRender = (threadId: ThreadId) =>
    Effect.suspend(() => {
      const entry = entryFor(threadId);
      if (entry.request === undefined) return Effect.void;
      if (entry.rendering) {
        entry.renderAgain = true;
        return Effect.void;
      }
      entry.rendering = true;
      const loop: Effect.Effect<void> = Effect.suspend(() => {
        const request = entry.request;
        entry.renderAgain = false;
        if (request === undefined) return Effect.void;
        return renderAll(threadId, request).pipe(
          Effect.andThen(Effect.suspend(() => (entry.renderAgain ? loop : Effect.void))),
        );
      });
      return loop.pipe(
        Effect.ensuring(
          Effect.sync(() => {
            entry.rendering = false;
          }).pipe(Effect.andThen(removeIfIdle(threadId, entry))),
        ),
        Effect.forkDetach,
        Effect.asVoid,
      );
    });

  /** Replaces the thread's pane roster; redraws when a pane opened or changed. */
  const applyRoster = (threadId: ThreadId, roster: unknown) =>
    Effect.suspend(() => {
      const previous = entryFor(threadId).snapshot.panes;
      const panes = panesFromRoster(roster, previous);
      if (panes === null || samePanes(panes, previous)) return Effect.void;
      return update(threadId, (snapshot) => ({ ...snapshot, panes })).pipe(
        Effect.andThen(requestRender(threadId)),
      );
    });

  /** Registers a thread's live query, reads its pane roster and draws. */
  const attach: ClaudePluginUiShape["attach"] = (threadId, request) =>
    Effect.suspend(() => {
      entryFor(threadId).request = request;
      return requestRender(threadId).pipe(
        // A pane opened before the stream armed is never pushed, so read the roster once.
        Effect.andThen(
          send(threadId, request, { subtype: "ui_panes" }).pipe(
            Effect.flatMap((response) =>
              threads.get(threadId)?.request === request
                ? applyRoster(threadId, responseOf(response))
                : Effect.void,
            ),
            Effect.forkDetach,
          ),
        ),
        Effect.asVoid,
      );
    });

  /** Clears a thread's plugin UI when the query that attached it closes. */
  const detach: ClaudePluginUiShape["detach"] = (threadId, request) =>
    Effect.suspend(() => {
      const entry = threads.get(threadId);
      // A replacement query may already have attached; only its own close clears it.
      if (entry === undefined || entry.request !== request) return Effect.void;
      entry.request = undefined;
      return update(threadId, () => EMPTY_CLAUDE_PLUGIN_UI_SNAPSHOT).pipe(
        Effect.andThen(removeIfIdle(threadId, entry)),
      );
    });

  /** Applies one `ui_status`, `ui_toast`, `ui_panes` or `ui_invalidate` message from the current query. */
  const ingest: ClaudePluginUiShape["ingest"] = (threadId, message, from) => {
    if (!isRecord(message) || message.type !== "system") return Effect.void;
    // A late message from a replaced query must not draw over the current one.
    if (threads.get(threadId)?.request !== from) return Effect.void;
    switch (message.subtype) {
      case "ui_status": {
        if (typeof message.plugin !== "string") return Effect.void;
        const plugin = message.plugin;
        const text = typeof message.text === "string" && message.text !== "" ? message.text : null;
        return update(threadId, (snapshot) => {
          const others = snapshot.statuses.filter((status) => status.plugin !== plugin);
          if (text === null && others.length === snapshot.statuses.length) return snapshot;
          return {
            ...snapshot,
            statuses: text === null ? others : [...others, { plugin, text }],
          };
        });
      }
      case "ui_toast": {
        if (typeof message.plugin !== "string" || typeof message.text !== "string") {
          return Effect.void;
        }
        const toast = {
          id: typeof message.uuid === "string" ? message.uuid : `toast-${++toastCount}`,
          plugin: message.plugin,
          text: message.text,
          timeoutMs: typeof message.timeout_ms === "number" ? message.timeout_ms : 4000,
        };
        return update(threadId, (snapshot) => ({ ...snapshot, toast }));
      }
      case "ui_panes":
        return applyRoster(threadId, message);
      case "ui_invalidate":
        return requestRender(threadId);
      default:
        return Effect.void;
    }
  };

  /** The thread's current snapshot, then the latest one after each change. */
  const subscribe: ClaudePluginUiShape["subscribe"] = (threadId) =>
    Stream.unwrap(
      Effect.gen(function* () {
        const signal = yield* Queue.sliding<void>(1);
        let signals = watchers.get(threadId);
        if (signals === undefined) {
          signals = new Set();
          watchers.set(threadId, signals);
        }
        const own = signals;
        own.add(signal);
        yield* Effect.addFinalizer(() =>
          Effect.sync(() => {
            own.delete(signal);
            if (own.size === 0 && watchers.get(threadId) === own) watchers.delete(threadId);
          }),
        );
        const current = () => threads.get(threadId)?.snapshot ?? EMPTY_CLAUDE_PLUGIN_UI_SNAPSHOT;
        return Stream.concat(
          Stream.make(current()),
          Stream.fromQueue(signal).pipe(Stream.map(() => current())),
        );
      }),
    );

  /** Forwards a plugin Button press to the thread's live query. */
  const press: ClaudePluginUiShape["press"] = (input) =>
    Effect.suspend(() => {
      const request = threads.get(input.threadId)?.request;
      if (request === undefined) return Effect.void;
      return send(input.threadId, request, {
        subtype: "ui_press",
        plugin: input.plugin,
        handle: input.handle,
        surface: "desktop",
        ...(input.key === undefined ? {} : { key: input.key }),
      }).pipe(Effect.asVoid);
    });

  /** Asks the CLI to close a pane; its `ui.close` hooks may keep it open, and the roster push says which. */
  const closePane: ClaudePluginUiShape["closePane"] = (input) =>
    Effect.suspend(() => {
      const request = threads.get(input.threadId)?.request;
      if (request === undefined) return Effect.void;
      return send(input.threadId, request, { subtype: "ui_close", id: input.paneId }).pipe(
        Effect.asVoid,
      );
    });

  return ClaudePluginUi.of({ attach, detach, ingest, subscribe, press, closePane });
});

/** Provides the shared ClaudePluginUi service. */
export const layer = Layer.effect(ClaudePluginUi, make);
