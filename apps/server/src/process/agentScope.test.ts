// @effect-diagnostics nodeBuiltinImport:off -- fake systemd binaries live in a temp dir.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, expect, it } from "@effect/vitest";
import {
  DEFAULT_SERVER_SETTINGS,
  type ServerSettings as ServerSettingsValue,
  type ServerSettingsError,
} from "@t3tools/contracts";
import { AgentScope } from "@t3tools/shared/AgentScope";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { vi } from "vite-plus/test";
import { ChildProcess, ChildProcessSpawner } from "effect/process";

import * as ServerConfig from "../config.ts";
import { makeCodexAppServerSpawnCommand } from "../orchestration-v2/Adapters/CodexAdapterV2.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as ServerSettings from "../serverSettings.ts";
import {
  agentSliceMemoryLimits,
  classifyScope,
  make,
  ownedAgentSlice,
  type ScopeState,
} from "./agentScope.ts";

const running: ScopeState = {
  loadState: "loaded",
  activeState: "active",
  result: "success",
  oomKills: 0,
  populated: true,
};

const GiB = 1024 ** 3;

const totalMemory = vi.hoisted(() => ({ bytes: 16 * 1024 ** 3 }));
vi.mock("node:os", async (importOriginal) => {
  const actual = await importOriginal<typeof import("node:os")>();
  return { ...actual, totalmem: () => totalMemory.bytes };
});

describe("agentSliceMemoryLimits", () => {
  it("leaves headroom on auto, sets no cap when off, and throttles below a fixed cap", () => {
    expect(agentSliceMemoryLimits(16 * GiB, "auto")).toEqual({ high: 10 * GiB, max: 12 * GiB });
    expect(agentSliceMemoryLimits(16 * GiB, "off")).toEqual({
      high: "infinity",
      max: "infinity",
    });
    expect(agentSliceMemoryLimits(16 * GiB, 64)).toEqual({ high: 62 * GiB, max: 64 * GiB });
    expect(agentSliceMemoryLimits(16 * GiB, 4)).toEqual({ high: 3 * GiB, max: 4 * GiB });
  });
});

describe("classifyScope", () => {
  it("reports an OOM kill only once systemd records it", () => {
    expect(classifyScope({ ...running, activeState: "failed", result: "oom-kill" })).toBe(
      "oom-killed",
    );
    expect(classifyScope({ ...running, oomKills: 1 })).toBe("stopping");
    expect(classifyScope({ ...running, populated: false })).toBe("stopping");
    expect(classifyScope({ ...running, activeState: "deactivating" })).toBe("stopping");
  });
});

const withFakeSystemd = <A, E>(
  body: (input: {
    readonly scope: Effect.Success<typeof make>;
    readonly units: Map<string, { activeState: string; result: string }>;
    readonly calls: ReadonlyArray<ReadonlyArray<string>>;
    readonly stateDir: string;
  }) => Effect.Effect<A, E>,
  {
    oomPolicy = true,
    failedSetProperties = 0,
    settings = ServerSettings.layerTest(),
  }: {
    readonly oomPolicy?: boolean;
    readonly failedSetProperties?: number;
    readonly settings?: Layer.Layer<ServerSettings.ServerSettingsService, ServerSettingsError>;
  } = {},
) =>
  Effect.gen(function* () {
    const bin = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-agent-scope-"));
    for (const name of ["systemd-run", "systemctl", "agent"]) {
      NodeFS.writeFileSync(NodePath.join(bin, name), "", { mode: 0o755 });
    }
    const units = new Map<string, { activeState: string; result: string }>();
    const calls: Array<ReadonlyArray<string>> = [];
    const output = (stdout: string, code = 0): ProcessRunner.ProcessRunOutput => ({
      stdout,
      stderr: "",
      code: ChildProcessSpawner.ExitCode(code),
      timedOut: false,
      stdoutTruncated: false,
      stderrTruncated: false,
      stdoutInvalidUtf8: false,
      stderrInvalidUtf8: false,
    });
    const runner = ProcessRunner.ProcessRunner.of({
      run: ({ args }) =>
        Effect.sync(() => {
          calls.push(args);
          if (!oomPolicy && args.some((arg) => arg.startsWith("--property=OOMPolicy="))) {
            return output("", 1);
          }
          if (args[1] === "set-property" && failedSetProperties > 0) {
            failedSetProperties -= 1;
            return output("", 1);
          }
          const [, verb, unit = ""] = args;
          if (verb === "reset-failed") units.delete(unit);
          if (verb !== "show") return output("");
          const state = units.get(unit);
          return output(
            state === undefined
              ? "LoadState=not-found\nActiveState=inactive\nResult=success\n"
              : `LoadState=loaded\nActiveState=${state.activeState}\nResult=${state.result}\n`,
          );
        }),
    });
    const scope = yield* make.pipe(
      Effect.provideService(ProcessRunner.ProcessRunner, runner),
      Effect.provideService(HostProcess.Platform, "linux"),
      Effect.provideService(HostProcess.Environment, { PATH: bin, XDG_RUNTIME_DIR: "/run/user/1" }),
    );
    const { stateDir } = yield* ServerConfig.ServerConfig;
    return yield* body({ scope, units, calls, stateDir }).pipe(
      Effect.ensuring(Effect.sync(() => NodeFS.rmSync(bin, { recursive: true, force: true }))),
    );
  }).pipe(
    Effect.scoped,
    Effect.provide(
      Layer.mergeAll(
        ServerConfig.layerTest(process.cwd(), { prefix: "t3-agent-scope-" }),
        settings,
      ).pipe(Layer.provideMerge(NodeServices.layer)),
    ),
  );

const unitOf = (launch: { readonly args: ReadonlyArray<string> }) =>
  launch.args.find((arg) => arg.startsWith("--unit="))?.slice("--unit=".length) ?? "";

const sliceOf = (launch: { readonly args: ReadonlyArray<string> }) =>
  launch.args.find((arg) => arg.startsWith("--slice="))?.slice("--slice=".length) ?? "";

const launchCodex = (scope: Effect.Success<typeof make>, threadId: string) =>
  makeCodexAppServerSpawnCommand({ command: "agent", args: ["app-server"], threadId }).pipe(
    Effect.provideService(AgentScope, scope),
    Effect.provideService(HostProcess.Platform, "linux"),
    Effect.map((command) => {
      assert(ChildProcess.isStandardCommand(command));
      return command;
    }),
  );

/**
 * The first read returns "auto" after the setting has already changed to
 * "off" and the change was published, as when a client saves the setting
 * while the first agent is setting up scopes.
 */
const settingsChangedDuringFirstRead = Layer.effect(
  ServerSettings.ServerSettingsService,
  Effect.gen(function* () {
    const base = yield* ServerSettings.ServerSettingsService;
    const changes = yield* PubSub.unbounded<ServerSettingsValue>();
    const off = { ...(yield* base.getSettings), agentMemoryLimit: "off" as const };
    let reads = 0;
    return ServerSettings.ServerSettingsService.of({
      ...base,
      getSettings: Effect.gen(function* () {
        reads += 1;
        if (reads > 1) return off;
        yield* PubSub.publish(changes, off);
        for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
        return { ...off, agentMemoryLimit: "auto" as const };
      }),
      subscribeChanges: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
    });
  }),
).pipe(Layer.provide(ServerSettings.layerTest()));

/** Test settings whose change stream is `changes`. */
const settingsWithChanges = (
  changes: PubSub.PubSub<ServerSettingsValue>,
  overrides: Parameters<typeof ServerSettings.layerTest>[0],
) =>
  Layer.effect(
    ServerSettings.ServerSettingsService,
    Effect.gen(function* () {
      const base = yield* ServerSettings.ServerSettingsService;
      return ServerSettings.ServerSettingsService.of({
        ...base,
        subscribeChanges: PubSub.subscribe(changes).pipe(Effect.map(Stream.fromSubscription)),
      });
    }),
  ).pipe(Layer.provide(ServerSettings.layerTest(overrides)));

describe("AgentScope service", () => {
  it.effect("labels a thread from its newest scope until its next session clears it", () =>
    withFakeSystemd(({ scope, units }) =>
      Effect.gen(function* () {
        const first = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        units.set(unitOf(first), { activeState: "failed", result: "oom-kill" });
        const second = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        units.set(unitOf(second), { activeState: "active", result: "success" });
        expect(yield* scope.oomKilled("a")).toBe(false);

        units.set(unitOf(second), { activeState: "failed", result: "oom-kill" });
        expect(yield* scope.oomKilled("a")).toBe(true);
        units.delete(unitOf(second));
        expect(yield* scope.oomKilled("a")).toBe(true);

        yield* scope.clear("a");
        expect(yield* scope.oomKilled("a")).toBe(false);
      }),
    ),
  );

  it.effect(
    "keeps shared Codex scopes running without attributing their OOM to the launch thread",
    () =>
      withFakeSystemd(({ scope, units, calls }) =>
        Effect.gen(function* () {
          const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          expect(agent.args).toContain("--property=OOMPolicy=stop");
          units.set(unitOf(agent), { activeState: "failed", result: "oom-kill" });
          expect(yield* scope.oomKilled("a")).toBe(true);

          const codex = yield* launchCodex(scope, "a");
          expect(codex.args).toContain("--property=OOMPolicy=continue");
          expect(codex.args).not.toContain("--property=OOMPolicy=stop");
          expect(codex.args).toContain("--property=TimeoutStopSec=10s");
          expect(codex.args).toContain("--collect");
          units.set(unitOf(codex), { activeState: "active", result: "success" });
          expect(yield* scope.oomKilled("a")).toBe(false);
          units.set(unitOf(codex), { activeState: "failed", result: "oom-kill" });
          expect(yield* scope.oomKilled("a")).toBe(false);
          expect(
            calls.filter(([, verb, unit]) => verb === "show" && unit === unitOf(codex)),
          ).toEqual([]);
        }),
      ),
  );

  it.effect("keeps a running scope while hundreds of other threads launch", () =>
    withFakeSystemd(({ scope, units }) =>
      Effect.gen(function* () {
        const live = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "live" });
        units.set(unitOf(live), { activeState: "active", result: "success" });
        for (let index = 0; index < 600; index++) {
          yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: `other-${index}` });
        }
        units.set(unitOf(live), { activeState: "failed", result: "oom-kill" });
        expect(yield* scope.oomKilled("live")).toBe(true);
      }),
    ),
  );

  it.effect("stops agents a killed server left before the first launch", () =>
    withFakeSystemd(({ scope, units, calls, stateDir }) =>
      Effect.gen(function* () {
        units.set(ownedAgentSlice(stateDir), { activeState: "active", result: "success" });
        const agent = yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        const stops = calls.filter(([, verb]) => verb === "stop");
        expect(stops).toEqual([["--user", "stop", ownedAgentSlice(stateDir)]]);
        expect(sliceOf(agent)).toBe(ownedAgentSlice(stateDir));
        expect(agent.args).toContain("--property=TimeoutStopSec=10s");
        const terminal = yield* scope.wrap({
          command: "agent",
          args: [],
          name: "terminal",
          survivesRestart: true,
        });
        expect(sliceOf(terminal)).toBe("app-t3code-agents.slice");
        expect(calls.filter(([, verb]) => verb === "stop")).toHaveLength(1);
      }),
    ),
  );

  it.effect("caps the agents slice from the memory limit setting", () =>
    withFakeSystemd(
      ({ scope, calls }) =>
        Effect.gen(function* () {
          yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          expect(calls.filter(([, verb]) => verb === "set-property")).toEqual([
            [
              "--user",
              "set-property",
              "--runtime",
              "app-t3code-agents.slice",
              "MemoryHigh=infinity",
              "MemoryMax=infinity",
            ],
          ]);
        }),
      { settings: ServerSettings.layerTest({ agentMemoryLimit: "off" }) },
    ),
  );

  it.effect("applies a setting changed while the first agent sets up scopes", () =>
    withFakeSystemd(
      ({ scope, calls }) =>
        Effect.gen(function* () {
          yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
          for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
          const limits = calls.filter(([, verb]) => verb === "set-property");
          expect(limits.at(-1)?.slice(4)).toEqual(["MemoryHigh=infinity", "MemoryMax=infinity"]);
        }),
      { settings: settingsChangedDuringFirstRead },
    ),
  );

  it.effect("retries memory limits that systemd did not accept", () =>
    Effect.gen(function* () {
      const changes = yield* PubSub.unbounded<ServerSettingsValue>();
      yield* withFakeSystemd(
        ({ scope, calls }) =>
          Effect.gen(function* () {
            yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
            yield* PubSub.publish(changes, {
              ...DEFAULT_SERVER_SETTINGS,
              agentMemoryLimit: "off",
            });
            for (let i = 0; i < 20; i++) yield* Effect.yieldNow;
            const limits = calls.filter(([, verb]) => verb === "set-property");
            expect(limits.map((args) => args.slice(4))).toEqual([
              ["MemoryHigh=infinity", "MemoryMax=infinity"],
              ["MemoryHigh=infinity", "MemoryMax=infinity"],
            ]);
          }),
        {
          settings: settingsWithChanges(changes, { agentMemoryLimit: "off" }),
          failedSetProperties: 1,
        },
      );
    }),
  );

  it.effect("follows RAM that grows while the server runs on auto", () =>
    withFakeSystemd(({ scope, calls }) =>
      Effect.gen(function* () {
        const memoryLimits = () =>
          calls.filter(([, verb]) => verb === "set-property").map((args) => args.slice(4));
        totalMemory.bytes = 8 * GiB;
        yield* scope.wrap({ command: "agent", args: [], name: "t", threadId: "a" });
        expect(memoryLimits()).toEqual([[`MemoryHigh=${4 * GiB}`, `MemoryMax=${6 * GiB}`]]);

        yield* TestClock.adjust("1 minute");
        expect(memoryLimits()).toHaveLength(1);

        totalMemory.bytes = 16 * GiB;
        yield* TestClock.adjust("1 minute");
        expect(memoryLimits()).toEqual([
          [`MemoryHigh=${4 * GiB}`, `MemoryMax=${6 * GiB}`],
          [`MemoryHigh=${10 * GiB}`, `MemoryMax=${12 * GiB}`],
        ]);
      }),
    ),
  );
});
