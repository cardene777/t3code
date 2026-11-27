import type { ColdStorageImages, ThreadId } from "@t3tools/contracts";
import { commandIndicatesFailure, takeToolOutputImageData } from "@t3tools/shared/toolOutput";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Predicate from "effect/Predicate";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import { forkParked } from "../serverActivation.ts";
import { ServerSettingsService } from "../serverSettings.ts";
import * as DatabaseMaintenance from "./DatabaseMaintenance.ts";
import * as DatabaseStorage from "./DatabaseStorage.ts";
import {
  ColdStorageStore,
  type ColdStoredAttachment,
  type ColdStoredTurnItem,
} from "./ColdStorageStore.ts";

const RUN_INTERVAL = Duration.hours(24);
// Bound what one cold storage transaction holds in memory.
const BATCH_ITEMS = 16;
const BATCH_BYTES = 16 * 1024 * 1024;
// A larger file would not fit a batch, so it stays in the attachments folder.
const MAX_ATTACHMENT_BYTES = 64 * 1024 * 1024;

export interface ColdStorageRun {
  readonly movedItems: number;
  readonly movedBytes: number;
}

export class ColdStorage extends Context.Service<
  ColdStorage,
  {
    /**
     * Moves the command output, attachments, and images of settled and archived threads idle
     * for the set days into cold storage.
     */
    readonly run: Effect.Effect<ColdStorageRun>;
  }
>()("t3/persistence/ColdStorage") {}

interface Move {
  readonly turnItemId: string;
  readonly updatedAt: string;
  readonly payloadJson: string;
  readonly moved: ColdStoredTurnItem;
  readonly bytes: number;
}

interface AttachmentMove {
  readonly file: string;
  readonly moved: ColdStoredAttachment;
}

const isCanonicalBase64 = (data: string) => Buffer.from(data, "base64").toString("base64") === data;

const PayloadJson = Schema.fromJsonString(Schema.Unknown);
const decodePayload = Schema.decodeUnknownEffect(PayloadJson);
const encodePayload = Schema.encodeEffect(PayloadJson);

// The stored payload without what moves; null when nothing of the item moves.
const takeMovedParts = (
  turnItemId: string,
  threadId: string,
  item: unknown,
  dropImages: boolean,
): { readonly payload: object; readonly moved: ColdStoredTurnItem } | null => {
  if (!Predicate.isObject(item)) return null;
  if (item.type === "command_execution") {
    if (typeof item.output !== "string" || item.output === "") return null;
    const { output, ...kept } = item;
    const failed = commandIndicatesFailure({
      exitCode: typeof item.exitCode === "number" ? item.exitCode : undefined,
      outputIndicatesFailure: item.outputIndicatesFailure === true,
      output,
    });
    return {
      // The output decided failure only while it was here.
      payload: {
        ...kept,
        ...(failed ? { outputIndicatesFailure: true } : {}),
        fromColdStorage: true,
      },
      moved: { turnItemId, threadId, output },
    };
  }
  if (item.type === "dynamic_tool") {
    const { output, images } = takeToolOutputImageData(item.output);
    if (images.length === 0) return null;
    if (dropImages) return { payload: { ...item, output }, moved: { turnItemId, threadId } };
    // Bytes are stored decoded; data that would not encode back the same stays in place.
    if (!images.every((image) => isCanonicalBase64(image.data))) return null;
    return {
      payload: { ...item, output, fromColdStorage: true },
      moved: { turnItemId, threadId, images },
    };
  }
  return null;
};

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const store = yield* ColdStorageStore;
  const projections = yield* ProjectionStoreV2;
  const settings = yield* ServerSettingsService;
  const storage = yield* DatabaseStorage.DatabaseStorage;
  const config = yield* ServerConfig.ServerConfig;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const reclaimFiles = DatabaseMaintenance.reclaimFiles(path, config.dbPath);
  // When the last run started, so a restart does not run it again before the day is over.
  const ranAtPath = path.join(path.dirname(config.dbPath), "cold-storage-ran-at");
  const readRanAt = fs.readFileString(ranAtPath).pipe(
    Effect.map(Number),
    Effect.orElseSucceed(() => Number.NaN),
  );
  const activeRunStatuses = sql.literal(
    "('preparing', 'queued', 'starting', 'running', 'waiting')",
  );
  const hasActiveRun = Effect.map(
    sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE status IN ${activeRunStatuses} LIMIT 1`,
    (rows) => rows.length > 0,
  );

  // A database made before automatic shrinking keeps its size until one full VACUUM.
  // The restart that Settings' Reclaim runs does it; ask once, only while no turn runs.
  const switchToIncrementalWhenIdle = Effect.gen(function* () {
    const mode = yield* sql<{ readonly auto_vacuum: number }>`PRAGMA auto_vacuum`;
    if ((mode[0]?.auto_vacuum ?? 0) !== 0) return;
    if (yield* fs.exists(reclaimFiles.request)) return;
    // A failed repair waits for the person, who sees it in Settings.
    if (yield* fs.exists(reclaimFiles.failure)) return;
    const free = yield* sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`;
    if ((free[0]?.freelist_count ?? 0) === 0) return;
    if (yield* hasActiveRun) return;
    yield* storage.requestReclaim.pipe(
      Effect.catch((error) =>
        Effect.logDebug("Cold storage left database space unreclaimed", { reason: error.reason }),
      ),
    );
  });

  // Only a file the item names by its full path counts; another path cannot be checked.
  const viewedImageOnDisk = (item: unknown) =>
    Predicate.isObject(item) &&
    typeof item.viewedImagePath === "string" &&
    path.isAbsolute(item.viewedImagePath)
      ? fs.exists(item.viewedImagePath).pipe(Effect.orElseSucceed(() => false))
      : Effect.succeed(false);

  const dropsImagesOf = (item: unknown, images: ColdStorageImages) =>
    images === "keep-all"
      ? Effect.succeed(false)
      : images === "drop-all"
        ? Effect.succeed(true)
        : viewedImageOnDisk(item);

  const runWith = (betweenItems: Effect.Effect<void>) =>
    Effect.suspend(() => {
      let movedItems = 0;
      let movedBytes = 0;
      let movedAttachments = 0;
      const flush = Effect.fn("ColdStorage.flush")(function* (batch: ReadonlyArray<Move>) {
        if (batch.length === 0) return true;
        // Cold storage commits first; a stop between the two leaves the item in both.
        if (!(yield* store.put(batch.map((move) => move.moved)))) return false;
        yield* sql.withTransaction(
          Effect.forEach(batch, (move) =>
            Effect.gen(function* () {
              // A later update owns the item again and keeps what it wrote.
              const updated = yield* sql`
                  UPDATE orchestration_v2_projection_turn_items SET payload_json = ${move.payloadJson}
                  WHERE turn_item_id = ${move.turnItemId} AND updated_at = ${move.updatedAt}
                  RETURNING turn_item_id`;
              if (updated.length === 0) return;
              movedItems++;
              movedBytes += move.bytes;
            }),
          ),
        );
        return true;
      });
      const flushAttachments = Effect.fn("ColdStorage.flushAttachments")(function* (
        batch: ReadonlyArray<AttachmentMove>,
      ) {
        if (batch.length === 0) return true;
        if (
          !(yield* store.put(
            [],
            batch.map((move) => move.moved),
          ))
        )
          return false;
        for (const move of batch) {
          const removed = yield* fs.remove(move.file, { force: true }).pipe(
            Effect.as(true),
            Effect.orElseSucceed(() => false),
          );
          if (removed) movedAttachments++;
        }
        return true;
      });

      const moveItems = Effect.fn("ColdStorage.moveItems")(function* (
        threadId: ThreadId,
        images: ColdStorageImages,
      ) {
        const items = yield* sql<{ readonly turn_item_id: string }>`
          SELECT turn_item_id FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId} AND type IN ('command_execution', 'dynamic_tool')
          ORDER BY ordinal`;
        let intact = true;
        let batch: Array<Move> = [];
        let batchBytes = 0;
        for (const { turn_item_id: turnItemId } of items) {
          const [row] = yield* sql<{
            readonly updated_at: string;
            readonly payload_json: string;
          }>`
            SELECT updated_at, payload_json FROM orchestration_v2_projection_turn_items
            WHERE turn_item_id = ${turnItemId}`;
          const item =
            row === undefined
              ? Option.none()
              : yield* decodePayload(row.payload_json).pipe(Effect.option);
          const moved = Option.isSome(item)
            ? takeMovedParts(
                turnItemId,
                threadId,
                item.value,
                yield* dropsImagesOf(item.value, images),
              )
            : null;
          if (row !== undefined && moved !== null) {
            const payloadJson = yield* encodePayload(moved.payload);
            const bytes = Buffer.byteLength(row.payload_json) - Buffer.byteLength(payloadJson);
            batch.push({
              turnItemId,
              updatedAt: row.updated_at,
              payloadJson,
              moved: moved.moved,
              bytes,
            });
            batchBytes += bytes;
            if (batch.length >= BATCH_ITEMS || batchBytes >= BATCH_BYTES) {
              intact = (yield* flush(batch)) && intact;
              batch = [];
              batchBytes = 0;
            }
          }
          yield* betweenItems;
        }
        return (yield* flush(batch)) && intact;
      });

      const moveAttachments = Effect.fn("ColdStorage.moveAttachments")(function* (
        threadId: ThreadId,
      ) {
        const ids = yield* projections.getThreadAttachmentIds(threadId, {
          includeShownByLiveFork: true,
        });
        let intact = true;
        let batch: Array<AttachmentMove> = [];
        let batchBytes = 0;
        for (const attachmentId of ids) {
          const file = resolveAttachmentPathById({
            attachmentsDir: config.attachmentsDir,
            attachmentId,
          });
          if (file === null) continue;
          const info = yield* fs.stat(file).pipe(Effect.option);
          if (
            Option.isNone(info) ||
            info.value.type !== "File" ||
            Number(info.value.size) > MAX_ATTACHMENT_BYTES
          ) {
            continue;
          }
          const bytes = yield* fs.readFile(file).pipe(Effect.option);
          if (Option.isNone(bytes)) continue;
          batch.push({
            file,
            moved: { attachmentId, threadId, fileName: path.basename(file), bytes: bytes.value },
          });
          batchBytes += bytes.value.byteLength;
          if (batch.length >= BATCH_ITEMS || batchBytes >= BATCH_BYTES) {
            intact = (yield* flushAttachments(batch)) && intact;
            batch = [];
            batchBytes = 0;
          }
          yield* betweenItems;
        }
        return (yield* flushAttachments(batch)) && intact;
      });

      return Effect.gen(function* () {
        const startedAt = yield* Clock.currentTimeMillis;
        const { coldStorageAfterDays, coldStorageImages } = (yield* settings.getSettings)
          .storageCleanup;
        if (coldStorageAfterDays === null) return;
        const idleSince = DateTime.formatIso(
          DateTime.makeUnsafe(startedAt - Duration.toMillis(Duration.days(coldStorageAfterDays))),
        );
        // Matches the sidebar's settled threads: a settled time, or a person's override.
        const threads = yield* sql<{ readonly thread_id: ThreadId; readonly updated_at: string }>`
          SELECT thread.thread_id, thread.updated_at FROM orchestration_v2_projection_threads AS thread
          WHERE thread.updated_at <= ${idleSince}
            AND (
              thread.archived_at IS NOT NULL
              OR json_extract(thread.payload_json, '$.settledAt') IS NOT NULL
              OR json_extract(thread.payload_json, '$.settledOverride') IS 'settled'
            )
            AND NOT EXISTS (
              SELECT 1 FROM orchestration_v2_projection_runs AS run
              WHERE run.thread_id = thread.thread_id AND run.status IN ${activeRunStatuses}
            )`;
        // A thread moved since its last activity has nothing left to move.
        const moved = yield* store.movedThreads;
        for (const { thread_id: threadId, updated_at: updatedAt } of threads) {
          if (moved.get(threadId) === updatedAt) continue;
          const itemsIntact = yield* moveItems(threadId, coldStorageImages);
          const attachmentsIntact = yield* moveAttachments(threadId);
          if (itemsIntact && attachmentsIntact) yield* store.markMoved(threadId, updatedAt);
        }
        yield* fs.writeFileString(ranAtPath, String(startedAt)).pipe(Effect.ignore);
        if (movedItems > 0 || movedAttachments > 0) {
          yield* Effect.logInfo("Moved idle threads into cold storage", {
            movedItems,
            movedBytes,
            movedAttachments,
          });
        }
        yield* switchToIncrementalWhenIdle;
      }).pipe(
        Effect.catch((cause) => Effect.logWarning("Cold storage run failed", { cause })),
        Effect.map(() => ({ movedItems, movedBytes })),
        Effect.withSpan("ColdStorage.run"),
      );
    });

  // Keep the daily run at 10% duty, as compaction does.
  const pacedRun = Effect.suspend(() => {
    let sliceStarted: bigint | undefined;
    return runWith(
      Effect.gen(function* () {
        const now = yield* Clock.monotonicTimeNanos;
        sliceStarted ??= now;
        const elapsed = now - sliceStarted;
        if (elapsed < 25_000_000n) return;
        yield* Effect.sleep((Number(elapsed) / 1_000_000) * 9);
        sliceStarted = yield* Clock.monotonicTimeNanos;
      }),
    );
  });

  const intervalMs = Duration.toMillis(RUN_INTERVAL);
  const lastStartedAt = yield* readRanAt;
  const untilDueMs = Number.isFinite(lastStartedAt)
    ? lastStartedAt + intervalMs - (yield* Clock.currentTimeMillis)
    : 0;
  // Leave startup to the work the person opened the app for.
  const firstRunDelay = Duration.millis(
    Math.min(intervalMs, Math.max(Duration.toMillis(Duration.minutes(10)), untilDueMs)),
  );
  yield* forkParked(
    Effect.sleep(firstRunDelay).pipe(
      Effect.andThen(pacedRun.pipe(Effect.repeat(Schedule.spaced(RUN_INTERVAL)))),
    ),
  );

  return ColdStorage.of({ run: runWith(Effect.void) });
});

export const layer = Layer.effect(ColdStorage, make);
