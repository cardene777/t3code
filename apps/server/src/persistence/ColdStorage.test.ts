import * as NodeSqlite from "node:sqlite";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  RunId,
  ThreadId,
  TurnItemId,
  type ChatAttachment,
  type ModelSelection,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import { toolOutputImages } from "@t3tools/shared/toolOutput";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import * as SqlClient from "effect/sql/SqlClient";

import { resolveAttachmentPath } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as ProjectStore from "../orchestration-v2/ProjectStore.ts";
import * as ThreadSearch from "../orchestration-v2/ThreadSearch.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as ColdStorage from "./ColdStorage.ts";
import * as ColdStorageStore from "./ColdStorageStore.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as DatabaseStorage from "./DatabaseStorage.ts";
import * as Sqlite from "./Sqlite.ts";

const DAY = 24 * 60 * 60 * 1000;
const TODAY = Date.UTC(2026, 10, 26);
const projectId = ProjectId.make("project:cold");
const providerInstanceId = ProviderInstanceId.make("codex");
const modelSelection = {
  instanceId: providerInstanceId,
  model: "gpt-5.4",
} satisfies ModelSelection;
const driver = ProviderDriverKind.make("codex");

// Image bytes that do not compress, like the PNG and JPEG files agents open.
const noise = (size: number, seed: number) => {
  const bytes = Buffer.alloc(size);
  let state = seed;
  for (let index = 0; index < size; index++) {
    state = (state * 1103515245 + 12345) >>> 0;
    bytes[index] = state >>> 24;
  }
  return bytes;
};

const layer = (
  options: {
    readonly restart?: Effect.Effect<void> | null;
    readonly withoutAutoVacuum?: boolean;
    readonly settings?: Parameters<typeof ServerSettingsService.layerTest>[0];
  } = {},
) => {
  const database = Layer.mergeAll(Sqlite.layerConfig, ColdStorageStore.layer).pipe(
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "cold-storage-" }).pipe(
        Layer.provideMerge(NodeServices.layer),
      ),
    ),
  );
  // A database made before automatic shrinking, which only a full VACUUM converts.
  const withoutAutoVacuum = Layer.effectDiscard(
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* sql`PRAGMA auto_vacuum = NONE`;
      yield* sql`VACUUM`;
    }),
  );
  return ColdStorage.layer.pipe(
    Layer.provideMerge(
      Layer.mergeAll(
        DatabaseStorage.layer.pipe(Layer.provide(ProjectionMaintenance.layer)),
        ProjectionStore.layer,
        ProjectStore.layer,
        ThreadSearch.layer,
        // Cold storage is off by default; these tests run it at the age its switch starts from.
        ServerSettingsService.layerTest({
          ...options.settings,
          storageCleanup: { coldStorageAfterDays: 7, ...options.settings?.storageCleanup },
        }),
      ),
    ),
    Layer.provide(Layer.succeed(DatabaseStorage.ServerRestart, options.restart ?? null)),
    Layer.provideMerge(
      options.withoutAutoVacuum === true
        ? withoutAutoVacuum.pipe(Layer.provideMerge(database))
        : database,
    ),
  );
};

interface Fixture {
  readonly threadId: ThreadId;
  readonly commandId: TurnItemId;
  readonly imageId: TurnItemId;
  readonly commandOutput: string;
  readonly imageData: string;
}

interface StoredAttachment {
  readonly attachment: ChatAttachment;
  readonly bytes: Uint8Array;
}

/** A thread last active `idleDays` ago, with one command execution and one image an agent opened with Read. */
const addThread = Effect.fn("addThread")(function* (
  name: string,
  options: {
    readonly idleDays: number;
    readonly state?: "settled" | "archived" | "active";
    readonly runStatus?: "completed" | "running";
    readonly outputBytes?: number;
    readonly viewedImagePath?: string;
    readonly message?: string;
    readonly attachments?: ReadonlyArray<StoredAttachment>;
  },
) {
  const store = yield* ProjectionStore.ProjectionStoreV2;
  const at = TODAY - options.idleDays * DAY;
  const now = DateTime.makeUnsafe(at);
  const state = options.state ?? "settled";
  const runStatus = options.runStatus ?? "completed";
  const outputBytes = options.outputBytes ?? 200_000;
  const threadId = ThreadId.make(`thread:${name}`);
  const runId = RunId.make(`run:${name}`);
  const nodeId = NodeId.make(`node:${name}`);
  yield* store.apply({
    id: EventId.make(`event:${name}:thread`),
    type: "thread.created",
    threadId,
    occurredAt: now,
    payload: {
      createdBy: "user",
      creationSource: "web",
      id: threadId,
      projectId,
      title: name,
      providerInstanceId,
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      branch: null,
      worktreePath: null,
      activeProviderThreadId: null,
      lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
      forkedFrom: null,
      createdAt: now,
      updatedAt: now,
      archivedAt: state === "archived" ? now : null,
      settledOverride: state === "settled" ? "settled" : null,
      settledAt: state === "settled" ? now : null,
      lastVisitedAt: null,
      deletedAt: null,
    },
  });
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  yield* fs.makeDirectory(config.attachmentsDir, { recursive: true });
  for (const { attachment, bytes } of options.attachments ?? []) {
    const file = resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment });
    assert.isNotNull(file);
    if (file !== null) yield* fs.writeFile(file, bytes);
  }
  yield* store.apply({
    id: EventId.make(`event:${name}:message`),
    type: "message.updated",
    threadId,
    providerInstanceId,
    occurredAt: now,
    payload: {
      createdBy: "user",
      creationSource: "web",
      id: MessageId.make(`message:${name}`),
      threadId,
      runId: null,
      nodeId: null,
      role: "user",
      text: options.message ?? `${name} request`,
      attachments: (options.attachments ?? []).map(({ attachment }) => attachment),
      streaming: false,
      createdAt: now,
      updatedAt: now,
    },
  });
  yield* store.apply({
    id: EventId.make(`event:${name}:run`),
    type: "run.created",
    threadId,
    runId,
    nodeId,
    driver,
    providerInstanceId,
    occurredAt: now,
    payload: {
      id: runId,
      threadId,
      ordinal: 1,
      providerInstanceId,
      modelSelection,
      providerThreadId: null,
      userMessageId: MessageId.make(`message:${name}`),
      rootNodeId: nodeId,
      activeAttemptId: null,
      status: runStatus,
      requestedAt: now,
      startedAt: now,
      completedAt: runStatus === "completed" ? now : null,
      checkpointId: null,
      contextHandoffId: null,
    },
  });
  const base = {
    threadId,
    runId,
    nodeId,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    status: "completed" as const,
    title: null,
    startedAt: now,
    completedAt: now,
    updatedAt: now,
  };
  const commandId = TurnItemId.make(`item:${name}:command`);
  const imageId = TurnItemId.make(`item:${name}:image`);
  const commandOutput = `${name} build line\n`.repeat(Math.ceil(outputBytes / (name.length + 12)));
  const imageData = noise(outputBytes, name.length).toString("base64");
  const item = (id: string, payload: object) =>
    store.apply({
      id: EventId.make(`event:${id}`),
      type: "turn-item.updated",
      threadId,
      runId,
      nodeId,
      driver,
      occurredAt: now,
      payload: payload as never,
    });
  yield* item(commandId, {
    ...base,
    id: commandId,
    ordinal: 1,
    type: "command_execution",
    input: "pnpm build",
    output: commandOutput,
    exitCode: 0,
  });
  yield* item(imageId, {
    ...base,
    id: imageId,
    ordinal: 2,
    type: "dynamic_tool",
    toolName: "Read",
    ...(options.viewedImagePath === undefined ? {} : { viewedImagePath: options.viewedImagePath }),
    input: { file_path: options.viewedImagePath ?? "screen.png" },
    output: [
      { type: "text", text: "Read screen.png" },
      { type: "image", source: { type: "base64", media_type: "image/png", data: imageData } },
    ],
  });
  return { threadId, commandId, imageId, commandOutput, imageData } satisfies Fixture;
});

const payloads = (threadId: ThreadId) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly turn_item_id: string; readonly payload_json: string }>`
      SELECT turn_item_id, payload_json FROM orchestration_v2_projection_turn_items
      WHERE thread_id = ${threadId} ORDER BY ordinal`,
  );

const payloadOf = (itemId: TurnItemId) =>
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly payload_json: string }>`
      SELECT payload_json FROM orchestration_v2_projection_turn_items
      WHERE turn_item_id = ${itemId}`,
  ).pipe(Effect.map((rows) => rows[0]?.payload_json ?? ""));

// Fails when the output and the image base64 are still in the database.
const assertMoved = (fixture: Fixture) =>
  Effect.gen(function* () {
    // The stored JSON escapes the output's line breaks, so look for its first line.
    const firstLine = fixture.commandOutput.slice(0, fixture.commandOutput.indexOf("\n"));
    assert.include(yield* payloadOf(fixture.commandId), '"type":"command_execution"');
    assert.notInclude(yield* payloadOf(fixture.commandId), firstLine);
    assert.notInclude(yield* payloadOf(fixture.imageId), fixture.imageData.slice(0, 200));
  });

const sidecarPath = (name: string) =>
  Effect.gen(function* () {
    const path = yield* Path.Path;
    const { dbPath } = yield* ServerConfig.ServerConfig;
    return path.join(path.dirname(dbPath), name);
  });

const coldStoragePath = sidecarPath("cold-storage.sqlite");

// Reads `cold-storage.sqlite` with its own connection, as a person inspecting the file would.
const coldStorageRows = <Row>(query: string, ...params: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const file = yield* coldStoragePath;
    assert.isTrue(yield* fs.exists(file), "cold-storage.sqlite exists next to the database");
    const database = new NodeSqlite.DatabaseSync(file, { readOnly: true });
    try {
      return database.prepare(query).all(...params) as unknown as ReadonlyArray<Row>;
    } finally {
      database.close();
    }
  });

const countIn = (table: string, column: string, value: string) =>
  coldStorageRows<{ readonly count: number }>(
    `SELECT COUNT(*) AS count FROM ${table} WHERE ${column} = ?`,
    value,
  ).pipe(Effect.map((rows) => rows[0]?.count ?? 0));

const fromColdStorage = (item: OrchestrationV2TurnItem | null) =>
  item !== null && "fromColdStorage" in item ? item.fromColdStorage : undefined;

const readBack = (fixture: Fixture) =>
  Effect.gen(function* () {
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const command = yield* store.getTurnItem({
      threadId: fixture.threadId,
      itemId: fixture.commandId,
    });
    const image = yield* store.getTurnItem({ threadId: fixture.threadId, itemId: fixture.imageId });
    return { command, image };
  });

const commandOutputOf = (item: OrchestrationV2TurnItem | null) =>
  item?.type === "command_execution" ? item.output : undefined;

const imagesOf = (item: OrchestrationV2TurnItem | null) =>
  item?.type === "dynamic_tool" ? toolOutputImages(item.output) : [];

const attachmentOf = (
  threadId: ThreadId,
  index: number,
  type: "image" | "file",
): StoredAttachment => {
  const segment = threadId.replace(/[^a-z0-9]+/g, "-");
  const uuid = `00000000-0000-4000-8000-${String(index).padStart(12, "0")}`;
  const bytes = noise(50_000 + index, index);
  return type === "image"
    ? {
        attachment: {
          type: "image",
          id: `${segment}-${uuid}`,
          name: "screen.png",
          mimeType: "image/png",
          sizeBytes: bytes.byteLength,
        },
        bytes,
      }
    : {
        attachment: {
          type: "file",
          id: `${segment}-${uuid}-html`,
          name: "report.html",
          mimeType: "text/html",
          sizeBytes: bytes.byteLength,
        },
        bytes,
      };
};

const attachmentFile = (attachment: ChatAttachment) =>
  Effect.gen(function* () {
    const config = yield* ServerConfig.ServerConfig;
    return resolveAttachmentPath({ attachmentsDir: config.attachmentsDir, attachment }) ?? "";
  });

it.effect("cold-storage-moves-settled-and-archived-threads", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const settled = yield* addThread("settled", { idleDays: 8, state: "settled" });
    const archived = yield* addThread("archived", { idleDays: 8, state: "archived" });
    const coldStorage = yield* ColdStorage.ColdStorage;
    const result = yield* coldStorage.run;
    assert.equal(result.movedItems, 4);
    for (const fixture of [settled, archived]) {
      yield* assertMoved(fixture);
      assert.equal(yield* countIn("outputs", "turn_item_id", fixture.commandId), 1);
    }
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-leaves-active-threads", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const active = yield* addThread("active", { idleDays: 400, state: "active" });
    const settled = yield* addThread("settled", { idleDays: 8 });
    const before = yield* payloads(active.threadId);
    const coldStorage = yield* ColdStorage.ColdStorage;
    const result = yield* coldStorage.run;
    yield* assertMoved(settled);
    assert.equal(result.movedItems, 2);
    assert.deepEqual(yield* payloads(active.threadId), before);
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-leaves-recent-and-running-threads", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const due = yield* addThread("due", { idleDays: 7 });
    const recent = yield* addThread("recent", { idleDays: 6 });
    const running = yield* addThread("running", { idleDays: 40, runStatus: "running" });
    const before = [...(yield* payloads(recent.threadId)), ...(yield* payloads(running.threadId))];
    const coldStorage = yield* ColdStorage.ColdStorage;
    const result = yield* coldStorage.run;
    // Only the thread idle for exactly the set days moves.
    yield* assertMoved(due);
    assert.equal(result.movedItems, 2);
    const after = [...(yield* payloads(recent.threadId)), ...(yield* payloads(running.threadId))];
    assert.deepEqual(after, before);
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-follows-the-days-setting", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const settings = yield* ServerSettingsService;
    const coldStorage = yield* ColdStorage.ColdStorage;
    const five = yield* addThread("five", { idleDays: 5 });
    assert.equal((yield* coldStorage.run).movedItems, 0);

    yield* settings.updateSettings({ storageCleanup: { coldStorageAfterDays: 3 } });
    assert.equal((yield* coldStorage.run).movedItems, 2);
    yield* assertMoved(five);

    const ten = yield* addThread("ten", { idleDays: 10 });
    const before = yield* payloads(ten.threadId);
    yield* settings.updateSettings({ storageCleanup: { coldStorageAfterDays: null } });
    assert.equal((yield* coldStorage.run).movedItems, 0);
    assert.deepEqual(yield* payloads(ten.threadId), before);
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-keeps-images-by-default", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const old = yield* addThread("old", { idleDays: 8 });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    yield* assertMoved(old);
    assert.equal(yield* countIn("images", "turn_item_id", old.imageId), 1);
    const images = imagesOf((yield* readBack(old)).image);
    assert.lengthOf(images, 1);
    assert.equal(images[0]?.data, old.imageData);
    assert.equal(images[0]?.mimeType, "image/png");
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-keeps-only-images-without-a-file", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const folder = yield* fs.makeTempDirectoryScoped({ prefix: "cold-storage-images-" });
    const onDisk = path.join(folder, "screen.png");
    yield* fs.writeFile(onDisk, noise(1_000, 7));
    const withFile = yield* addThread("with-file", { idleDays: 8, viewedImagePath: onDisk });
    const gone = path.join(folder, "gone.png");
    const withoutFile = yield* addThread("without-file", { idleDays: 8, viewedImagePath: gone });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;

    yield* assertMoved(withFile);
    assert.include(yield* payloadOf(withFile.imageId), onDisk);
    assert.equal(yield* countIn("images", "turn_item_id", withFile.imageId), 0);
    assert.isUndefined(imagesOf((yield* readBack(withFile)).image)[0]?.data);

    yield* assertMoved(withoutFile);
    assert.equal(yield* countIn("images", "turn_item_id", withoutFile.imageId), 1);
    assert.equal(imagesOf((yield* readBack(withoutFile)).image)[0]?.data, withoutFile.imageData);
  }).pipe(
    Effect.scoped,
    Effect.provide(
      layer({ settings: { storageCleanup: { coldStorageImages: "keep-without-file" } } }),
    ),
  ),
);

it.effect("cold-storage-drops-every-image-when-set", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const old = yield* addThread("old", { idleDays: 8, viewedImagePath: "/gone/screen.png" });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    yield* assertMoved(old);
    assert.include(yield* payloadOf(old.imageId), "/gone/screen.png");
    assert.equal(yield* countIn("outputs", "turn_item_id", old.commandId), 1);
    const images = yield* coldStorageRows<{ readonly count: number }>(
      "SELECT COUNT(*) AS count FROM images",
    );
    assert.equal(images[0]?.count, 0);
  }).pipe(
    Effect.provide(layer({ settings: { storageCleanup: { coldStorageImages: "drop-all" } } })),
  ),
);

it.effect("cold-storage-reads-command-output-back", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const old = yield* addThread("old", { idleDays: 8 });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    yield* assertMoved(old);
    assert.equal(commandOutputOf((yield* readBack(old)).command), old.commandOutput);
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-marks-items-read-from-cold-storage", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const old = yield* addThread("old", { idleDays: 8 });
    const recent = yield* addThread("recent", { idleDays: 5 });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    const moved = yield* readBack(old);
    assert.isTrue(fromColdStorage(moved.command));
    assert.isTrue(fromColdStorage(moved.image));
    const kept = yield* readBack(recent);
    assert.isUndefined(fromColdStorage(kept.command));
    assert.isUndefined(fromColdStorage(kept.image));
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-moves-attachments-and-serves-them", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const fs = yield* FileSystem.FileSystem;
    const threadId = ThreadId.make("thread:old");
    const attachments = [attachmentOf(threadId, 1, "file"), attachmentOf(threadId, 2, "image")];
    yield* addThread("old", { idleDays: 8, attachments });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    const store = yield* ColdStorageStore.ColdStorageStore;
    for (const { attachment, bytes } of attachments) {
      assert.isFalse(yield* fs.exists(yield* attachmentFile(attachment)), attachment.id);
      assert.equal(yield* countIn("attachments", "attachment_id", attachment.id), 1);
      const opened = yield* store.attachmentFile(attachment.id);
      assert.isNotNull(opened);
      assert.deepEqual(Buffer.from(yield* fs.readFile(opened ?? "")), Buffer.from(bytes));
    }
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-keeps-parts-that-do-not-read-back", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const fs = yield* FileSystem.FileSystem;
    const file = yield* coldStoragePath;
    assert.isTrue(yield* fs.exists(file), "cold-storage.sqlite is open beside the database");
    // A write that lands damaged, which only reading it back can catch.
    const database = new NodeSqlite.DatabaseSync(file);
    try {
      database.exec(`
        CREATE TRIGGER damage_outputs AFTER INSERT ON outputs WHEN NEW.thread_id = 'thread:broken'
        BEGIN UPDATE outputs SET output = zeroblob(8) WHERE turn_item_id = NEW.turn_item_id; END;
        CREATE TRIGGER damage_attachments AFTER INSERT ON attachments
          WHEN NEW.thread_id = 'thread:broken'
        BEGIN UPDATE attachments SET bytes = zeroblob(8) WHERE attachment_id = NEW.attachment_id; END;
      `);
    } finally {
      database.close();
    }
    const intact = yield* addThread("intact", { idleDays: 8 });
    const brokenAttachment = attachmentOf(ThreadId.make("thread:broken"), 3, "file");
    const broken = yield* addThread("broken", { idleDays: 8, attachments: [brokenAttachment] });
    const before = yield* payloads(broken.threadId);
    const coldStorage = yield* ColdStorage.ColdStorage;
    const result = yield* coldStorage.run;
    yield* assertMoved(intact);
    assert.equal(result.movedItems, 2);
    assert.deepEqual(yield* payloads(broken.threadId), before);
    const kept = yield* attachmentFile(brokenAttachment.attachment);
    assert.deepEqual(Buffer.from(yield* fs.readFile(kept)), Buffer.from(brokenAttachment.bytes));
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-yields-to-a-later-update", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const old = yield* addThread("old", { idleDays: 8 });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    const store = yield* ProjectionStore.ProjectionStoreV2;
    const moved = yield* store.getTurnItem({ threadId: old.threadId, itemId: old.commandId });
    assert.isTrue(fromColdStorage(moved));
    assert.equal(moved?.type, "command_execution");
    if (moved?.type !== "command_execution") return;
    const now = DateTime.makeUnsafe(TODAY);
    const { fromColdStorage: _fromColdStorage, ...stored } = moved;
    yield* store.apply({
      id: EventId.make("event:old:command:again"),
      type: "turn-item.updated",
      threadId: old.threadId,
      runId: RunId.make("run:old"),
      nodeId: NodeId.make("node:old"),
      driver,
      occurredAt: now,
      payload: { ...stored, output: "output written after the move", updatedAt: now },
    });
    const updated = yield* store.getTurnItem({ threadId: old.threadId, itemId: old.commandId });
    assert.equal(commandOutputOf(updated), "output written after the move");
    assert.isUndefined(fromColdStorage(updated));
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-shrinks-an-incremental-database", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const sql = yield* SqlClient.SqlClient;
    assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
    for (const name of ["first", "second", "third"]) {
      yield* addThread(name, { idleDays: 8, outputBytes: 1_000_000 });
    }
    // The size the file takes at the next checkpoint; stat lags while the checkpoint worker holds the lock.
    const fileBytes = Effect.gen(function* () {
      const [pages] = yield* sql<{ readonly page_count: number }>`PRAGMA page_count`;
      const [size] = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
      return (pages?.page_count ?? 0) * (size?.page_size ?? 0);
    });
    const before = yield* fileBytes;
    const coldStorage = yield* ColdStorage.ColdStorage;
    const result = yield* coldStorage.run;
    assert.isAbove(result.movedBytes, 5_000_000);
    const storage = yield* DatabaseStorage.DatabaseStorage;
    const freePages = Effect.map(
      sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`,
      (rows) => rows[0]?.freelist_count ?? 0,
    );
    while ((yield* freePages) > 0) yield* storage.reclaim;
    const after = yield* fileBytes;
    assert.isAtLeast(before - after, result.movedBytes * 0.8);
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-restarts-once-to-switch-auto-vacuum", () =>
  Effect.gen(function* () {
    const restarts = yield* Ref.make(0);
    yield* Effect.gen(function* () {
      yield* TestClock.setTime(TODAY);
      const sql = yield* SqlClient.SqlClient;
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const { dbPath } = yield* ServerConfig.ServerConfig;
      const request = DatabaseMaintenance.reclaimFiles(path, dbPath).request;
      assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 0 }]);
      yield* addThread("old", { idleDays: 8 });
      const busy = yield* addThread("busy", { idleDays: 1, runStatus: "running" });
      const coldStorage = yield* ColdStorage.ColdStorage;

      // A running turn would be cut by the restart, so nothing is asked.
      yield* coldStorage.run;
      yield* TestClock.adjust(1000);
      assert.isFalse(yield* fs.exists(request));
      assert.equal(yield* Ref.get(restarts), 0);

      yield* sql`UPDATE orchestration_v2_projection_runs SET status = 'completed'
        WHERE thread_id = ${busy.threadId}`;
      yield* coldStorage.run;
      assert.isTrue(yield* fs.exists(request));
      yield* TestClock.adjust(1000);
      assert.equal(yield* Ref.get(restarts), 1);

      // The repair runs when the server stops; until then the request stands, so no second restart.
      yield* coldStorage.run;
      yield* TestClock.adjust(1000);
      assert.equal(yield* Ref.get(restarts), 1);
    }).pipe(
      Effect.provide(
        layer({
          restart: Ref.update(restarts, (count) => count + 1),
          withoutAutoVacuum: true,
        }),
      ),
    );
  }),
);

it.effect("cold-storage-keeps-moved-threads-searchable", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const projects = yield* ProjectStore.ProjectStoreV2;
    const createdAt = DateTime.formatIso(DateTime.makeUnsafe(TODAY - 30 * DAY));
    yield* projects.apply({
      sequence: 0,
      eventId: EventId.make("event:project:cold"),
      aggregateKind: "project",
      aggregateId: projectId,
      occurredAt: createdAt,
      commandId: null,
      causationEventId: null,
      correlationId: null,
      metadata: {},
      type: "project.created",
      payload: {
        projectId,
        title: "cold",
        workspaceRoot: "/work/cold",
        defaultModelSelection: null,
        scripts: [],
        createdAt,
        updatedAt: createdAt,
      },
    });
    const old = yield* addThread("old", {
      idleDays: 8,
      message: "Why does the lighthouse keeper log every ship?",
    });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    yield* assertMoved(old);
    const search = yield* ThreadSearch.ThreadSearch;
    const found = yield* search.search({ query: "lighthouse keeper" });
    assert.deepEqual(
      found.matches.map((match) => match.threadId),
      [old.threadId],
    );
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-writes-cold-storage-sqlite", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const fs = yield* FileSystem.FileSystem;
    const old = yield* addThread("old", { idleDays: 8 });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    assert.equal(yield* countIn("outputs", "turn_item_id", old.commandId), 1);
    assert.isFalse(yield* fs.exists(yield* sidecarPath("archive.sqlite")));
  }).pipe(Effect.provide(layer())),
);

it.effect("cold-storage-reports-its-total-size", () =>
  Effect.gen(function* () {
    yield* TestClock.setTime(TODAY);
    const storage = yield* DatabaseStorage.DatabaseStorage;
    const firstStatus = yield* storage.status.pipe(
      Stream.filter((status) => status !== null),
      Stream.runHead,
    );
    assert.isNumber(Option.getOrNull(firstStatus)?.coldStorageBytes);
    yield* addThread("old", { idleDays: 8 });
    const coldStorage = yield* ColdStorage.ColdStorage;
    yield* coldStorage.run;
    const pages = yield* coldStorageRows<{ readonly bytes: number }>(
      "SELECT page_count * page_size AS bytes FROM pragma_page_count(), pragma_page_size()",
    );
    const bytes = pages[0]?.bytes ?? 0;
    // The moved image does not compress.
    assert.isAbove(bytes, 200_000);
    const reported = yield* storage.status.pipe(
      Stream.filter((status) => status?.coldStorageBytes === bytes),
      Stream.runHead,
      Effect.forkChild,
    );
    // Settings reads the status that the next storage pass publishes.
    yield* TestClock.adjust("1 minute");
    assert.isTrue(Option.isSome(yield* Fiber.join(reported)));
  }).pipe(Effect.provide(layer())),
);
