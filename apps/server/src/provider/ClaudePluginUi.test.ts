import { assert, describe, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Stream from "effect/Stream";

import * as ClaudePluginUi from "./ClaudePluginUi.ts";

const threadId = ThreadId.make("thread-plugin-ui");
const band = { type: "Box", children: [{ type: "Text", children: ["5h 42%"] }] };

const latest = (service: ClaudePluginUi.ClaudePluginUiShape) =>
  service.subscribe(threadId).pipe(Stream.runHead, Effect.map(Option.getOrThrow));

/** The first snapshot, current or upcoming, that satisfies `predicate`. */
const awaitSnapshot = (
  service: ClaudePluginUi.ClaudePluginUiShape,
  predicate: (snapshot: Effect.Success<ReturnType<typeof latest>>) => boolean,
) =>
  service
    .subscribe(threadId)
    .pipe(Stream.filter(predicate), Stream.runHead, Effect.map(Option.getOrThrow));

describe("ClaudePluginUi", () => {
  it.effect("keeps one status line per plugin and removes it on null", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const status = (plugin: string, text: string | null) =>
        service.ingest(threadId, { type: "system", subtype: "ui_status", plugin, text }, undefined);

      yield* status("tier-badge", "T1 small");
      yield* status("usage-deck", "ctx 40%");
      yield* status("tier-badge", "T2 feature");
      assert.deepStrictEqual((yield* latest(service)).statuses, [
        { plugin: "usage-deck", text: "ctx 40%" },
        { plugin: "tier-badge", text: "T2 feature" },
      ]);

      yield* status("tier-badge", null);
      assert.deepStrictEqual((yield* latest(service)).statuses, [
        { plugin: "usage-deck", text: "ctx 40%" },
      ]);
    }),
  );

  it.effect("keeps the latest toast with its id and timeout", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      yield* service.ingest(
        threadId,
        {
          type: "system",
          subtype: "ui_toast",
          plugin: "blast-radius",
          text: "3 files touched",
          timeout_ms: 2500,
          uuid: "toast-uuid",
        },
        undefined,
      );
      assert.deepStrictEqual((yield* latest(service)).toast, {
        id: "toast-uuid",
        plugin: "blast-radius",
        text: "3 files touched",
        timeoutMs: 2500,
      });
    }),
  );

  it.effect("asks the CLI for the AbovePrompt band on attach and on invalidate", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const requests: Array<Record<string, unknown>> = [];
      let hooked = true;
      const request: ClaudePluginUi.ClaudeControlRequest = async (body) => {
        requests.push(body);
        return { subtype: "success", request_id: "r1", response: { tree: band, hooked } };
      };

      yield* service.attach(threadId, request);
      const drawn = yield* awaitSnapshot(service, (snapshot) => snapshot.band !== null);
      assert.deepStrictEqual(drawn.band, band);
      assert.strictEqual(requests[0]?.subtype, "ui_render");
      assert.strictEqual(requests[0]?.surface, "desktop");
      assert.strictEqual(requests[0]?.component, "AbovePrompt");

      hooked = false;
      yield* service.ingest(threadId, { type: "system", subtype: "ui_invalidate" }, request);
      const cleared = yield* awaitSnapshot(service, (snapshot) => snapshot.band === null);
      assert.strictEqual(cleared.band, null);
    }),
  );

  it.effect("reads the pane roster on attach, draws each pane, and drops a closed one", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const paneTree = { type: "Text", children: ["Proceed?"] };
      const renders: Array<Record<string, unknown>> = [];
      const request: ClaudePluginUi.ClaudeControlRequest = async (body) => {
        if (body.subtype === "ui_panes") {
          return {
            response: {
              panes: [{ id: "guard", title: "Blast radius", plugin: "blast-radius", columns: 50 }],
              shown_id: "guard",
            },
          };
        }
        renders.push(body);
        return body.component === "Pane"
          ? { response: { tree: paneTree, hooked: true } }
          : { response: { hooked: false } };
      };

      yield* service.attach(threadId, request);
      const drawn = yield* awaitSnapshot(service, (snapshot) => snapshot.panes[0]?.tree != null);
      assert.deepStrictEqual(drawn.panes, [
        { id: "guard", plugin: "blast-radius", title: "Blast radius", columns: 50, tree: paneTree },
      ]);
      const paneRender = renders.find((body) => body.component === "Pane");
      assert.strictEqual(paneRender?.instance_id, "guard");
      assert.deepInclude(paneRender?.props, { bodyColumns: 50 });

      yield* service.ingest(threadId, { type: "system", subtype: "ui_panes", panes: [] }, request);
      assert.deepStrictEqual((yield* latest(service)).panes, []);
    }),
  );

  it.effect("ignores the engine's placeholder tree", () =>
    Effect.sync(() => {
      assert.strictEqual(
        ClaudePluginUi.treeFromRenderResponse({
          subtype: "success",
          response: { tree: { type: "engine", ref: 0 }, hooked: true },
        }),
        null,
      );
      assert.deepStrictEqual(
        ClaudePluginUi.treeFromRenderResponse({
          subtype: "success",
          response: { tree: band, hooked: true },
        }),
        band,
      );
    }),
  );

  it.effect("clears on detach only for the query that attached", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const first: ClaudePluginUi.ClaudeControlRequest = async () => ({ hooked: false });
      const second: ClaudePluginUi.ClaudeControlRequest = async () => ({ hooked: false });
      yield* service.attach(threadId, first);
      yield* service.ingest(
        threadId,
        { type: "system", subtype: "ui_status", plugin: "tier-badge", text: "T1" },
        first,
      );
      yield* service.attach(threadId, second);

      yield* service.detach(threadId, first);
      assert.strictEqual((yield* latest(service)).statuses.length, 1);

      yield* service.detach(threadId, second);
      assert.strictEqual((yield* latest(service)).statuses.length, 0);
    }),
  );

  it.effect("drops a render that resolves after its query detached", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const current = { type: "Text", children: ["current"] };
      let resolveStale: (value: unknown) => void = () => {};
      let markInFlight: () => void = () => {};
      const inFlight = new Promise<void>((resolve) => {
        markInFlight = resolve;
      });
      const stale: ClaudePluginUi.ClaudeControlRequest = (body) =>
        body.subtype === "ui_render"
          ? new Promise((resolve) => {
              resolveStale = resolve;
              markInFlight();
            })
          : Promise.resolve({ response: { panes: [] } });
      yield* service.attach(threadId, stale);
      yield* Effect.promise(() => inFlight);
      yield* service.detach(threadId, stale);
      // The replacement's render waits behind the stale one, so the first band drawn shows
      // whether the stale tree was dropped.
      yield* service.attach(threadId, async (body) =>
        body.subtype === "ui_render"
          ? { response: { tree: current, hooked: true } }
          : { response: { panes: [] } },
      );
      resolveStale({ subtype: "success", response: { tree: band, hooked: true } });
      const drawn = yield* awaitSnapshot(service, (snapshot) => snapshot.band !== null);
      assert.deepStrictEqual(drawn.band, current);
    }),
  );

  it.effect("ignores UI from a query that has been replaced", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const old: ClaudePluginUi.ClaudeControlRequest = async () => ({ hooked: false });
      const replacement: ClaudePluginUi.ClaudeControlRequest = async () => ({ hooked: false });
      const status = (text: string, from: ClaudePluginUi.ClaudeControlRequest) =>
        service.ingest(threadId, { type: "system", subtype: "ui_status", plugin: "p", text }, from);
      yield* service.attach(threadId, old);
      yield* service.attach(threadId, replacement);
      yield* status("late from the old query", old);
      assert.deepStrictEqual((yield* latest(service)).statuses, []);
      yield* status("current", replacement);
      assert.deepStrictEqual((yield* latest(service)).statuses, [{ plugin: "p", text: "current" }]);
    }),
  );

  it.effect("a slow subscriber gets the latest snapshot, not a backlog", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      let published = false;
      // While the subscriber handles its first snapshot, 100 updates land.
      const seen = yield* service.subscribe(threadId).pipe(
        Stream.tap(() =>
          published
            ? Effect.void
            : Effect.gen(function* () {
                published = true;
                for (let i = 0; i < 100; i += 1) {
                  yield* service.ingest(
                    threadId,
                    { type: "system", subtype: "ui_status", plugin: "p", text: `v${i}` },
                    undefined,
                  );
                }
              }),
        ),
        Stream.take(2),
        Stream.runCollect,
      );
      assert.deepStrictEqual([...seen][1]?.statuses, [{ plugin: "p", text: "v99" }]);
    }),
  );

  it.effect("forwards a pane close to the live query", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const requests: Array<Record<string, unknown>> = [];
      yield* service.attach(threadId, async (body) => {
        requests.push(body);
        return { hooked: false };
      });
      yield* service.closePane({ threadId, paneId: "guard" });
      assert.deepStrictEqual(
        requests.find((body) => body.subtype === "ui_close"),
        { subtype: "ui_close", id: "guard" },
      );
    }),
  );

  it.effect("forwards a press to the live query", () =>
    Effect.gen(function* () {
      const service = yield* ClaudePluginUi.make;
      const requests: Array<Record<string, unknown>> = [];
      yield* service.attach(threadId, async (body) => {
        requests.push(body);
        return { hooked: false };
      });
      yield* service.press({ threadId, plugin: "usage-deck", handle: 7, key: "refresh" });
      assert.deepStrictEqual(
        requests.find((body) => body.subtype === "ui_press"),
        {
          subtype: "ui_press",
          plugin: "usage-deck",
          handle: 7,
          surface: "desktop",
          key: "refresh",
        },
      );
    }),
  );
});
