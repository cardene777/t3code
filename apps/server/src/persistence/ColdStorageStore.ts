import * as NodeUtil from "node:util";
import * as NodeZlib from "node:zlib";

import type { OrchestrationV2TurnItem } from "@t3tools/contracts";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import { putToolOutputImageData, type IndexedToolOutputImage } from "@t3tools/shared/toolOutput";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { resolveAttachmentRelativePath } from "../attachmentPaths.ts";
import { resolveAttachmentPathById } from "../attachmentStore.ts";
import * as ServerConfig from "../config.ts";

const gzip = NodeUtil.promisify(NodeZlib.gzip);
const gunzip = NodeUtil.promisify(NodeZlib.gunzip);

export interface ColdStoredTurnItem {
  readonly turnItemId: string;
  readonly threadId: string;
  readonly output?: string;
  readonly images?: ReadonlyArray<IndexedToolOutputImage>;
}

export interface ColdStoredAttachment {
  readonly attachmentId: string;
  readonly threadId: string;
  /** The file's name in the attachments folder. */
  readonly fileName: string;
  readonly bytes: Uint8Array;
}

export class ColdStorageStore extends Context.Service<
  ColdStorageStore,
  {
    /**
     * Writes the moved parts in one transaction and reads them back before it commits.
     * False when they did not land intact; the database must keep them then.
     */
    readonly put: (
      items: ReadonlyArray<ColdStoredTurnItem>,
      attachments?: ReadonlyArray<ColdStoredAttachment>,
    ) => Effect.Effect<boolean>;
    /** Puts back the parts of a moved item that live in cold storage. */
    readonly restore: (item: OrchestrationV2TurnItem) => Effect.Effect<OrchestrationV2TurnItem>;
    /** The attachment's file, written back to the attachments folder when it was moved. */
    readonly attachmentFile: (attachmentId: string) => Effect.Effect<string | null>;
    /** The `updated_at` each thread had when its parts last moved. */
    readonly movedThreads: Effect.Effect<ReadonlyMap<string, string>>;
    readonly markMoved: (threadId: string, updatedAt: string) => Effect.Effect<void>;
    /** Bytes of `cold-storage.sqlite`; undefined while it cannot be read. */
    readonly size: Effect.Effect<number | undefined>;
  }
>()("t3/persistence/ColdStorageStore") {}

class ColdStorageCompressionError extends Schema.TaggedError<ColdStorageCompressionError>()(
  "ColdStorageCompressionError",
  { cause: Schema.Defect() },
) {}

class ColdStorageReadBackError extends Schema.TaggedError<ColdStorageReadBackError>()(
  "ColdStorageReadBackError",
  { id: Schema.String },
) {}

const compressionError = (cause: unknown) => new ColdStorageCompressionError({ cause });
const compress = (bytes: string | Uint8Array) =>
  Effect.tryPromise({ try: () => gzip(bytes), catch: compressionError });
const decompress = (bytes: Uint8Array) =>
  Effect.tryPromise({ try: () => gunzip(bytes), catch: compressionError });

const unavailable = (attachmentsDir: string) =>
  ColdStorageStore.of({
    put: Effect.fn("ColdStorageStore.put")(() => Effect.succeed(false)),
    restore: Effect.succeed,
    attachmentFile: Effect.fn("ColdStorageStore.attachmentFile")((attachmentId: string) =>
      Effect.sync(() => resolveAttachmentPathById({ attachmentsDir, attachmentId })),
    ),
    movedThreads: Effect.succeed(new Map()),
    markMoved: Effect.fn("ColdStorageStore.markMoved")(() => Effect.void),
    size: Effect.undefined,
  });

const open = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const { dbPath, attachmentsDir } = yield* ServerConfig.ServerConfig;
  const filename = path.join(path.dirname(dbPath), "cold-storage.sqlite");
  const context = yield* Layer.build(
    NodeSqliteClient.layer({
      filename,
      spanAttributes: { "db.name": path.basename(filename), "service.name": "t3code-server" },
    }),
  );
  const sql = Context.get(context, SqlClient.SqlClient);
  yield* sql`PRAGMA busy_timeout = 5000`;
  yield* sql`
    CREATE TABLE IF NOT EXISTS outputs (
      turn_item_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      moved_at INTEGER NOT NULL,
      output BLOB NOT NULL
    )`;
  // Decoded bytes, about a quarter smaller than the base64 the database held.
  yield* sql`
    CREATE TABLE IF NOT EXISTS images (
      turn_item_id TEXT NOT NULL,
      thread_id TEXT NOT NULL,
      image_index INTEGER NOT NULL,
      media_type TEXT NOT NULL,
      bytes BLOB NOT NULL,
      PRIMARY KEY (turn_item_id, image_index)
    )`;
  yield* sql`
    CREATE TABLE IF NOT EXISTS attachments (
      attachment_id TEXT PRIMARY KEY,
      thread_id TEXT NOT NULL,
      moved_at INTEGER NOT NULL,
      file_name TEXT NOT NULL,
      bytes BLOB NOT NULL
    )`;
  yield* sql`
    CREATE TABLE IF NOT EXISTS threads (
      thread_id TEXT PRIMARY KEY,
      updated_at TEXT NOT NULL
    )`;

  const readBack = Effect.fn("ColdStorageStore.readBack")(function* (
    items: ReadonlyArray<ColdStoredTurnItem>,
    attachments: ReadonlyArray<ColdStoredAttachment>,
  ) {
    for (const item of items) {
      if (item.output !== undefined) {
        const [row] = yield* sql<{ readonly output: Uint8Array }>`
          SELECT output FROM outputs WHERE turn_item_id = ${item.turnItemId}`;
        const stored = row === undefined ? undefined : yield* decompress(row.output);
        if (stored?.toString("utf8") !== item.output) {
          return yield* new ColdStorageReadBackError({ id: item.turnItemId });
        }
      }
      if (item.images === undefined) continue;
      const rows = yield* sql<{
        readonly image_index: number;
        readonly media_type: string;
        readonly bytes: Uint8Array;
      }>`
        SELECT image_index, media_type, bytes FROM images
        WHERE turn_item_id = ${item.turnItemId} ORDER BY image_index`;
      const intact =
        rows.length === item.images.length &&
        item.images.every(
          (image, index) =>
            rows[index]?.image_index === image.index &&
            rows[index]?.media_type === image.mimeType &&
            Buffer.from(rows[index]?.bytes ?? []).equals(Buffer.from(image.data, "base64")),
        );
      if (!intact) return yield* new ColdStorageReadBackError({ id: item.turnItemId });
    }
    for (const attachment of attachments) {
      const [row] = yield* sql<{ readonly bytes: Uint8Array }>`
        SELECT bytes FROM attachments WHERE attachment_id = ${attachment.attachmentId}`;
      const stored = row === undefined ? undefined : yield* decompress(row.bytes);
      if (stored === undefined || !stored.equals(Buffer.from(attachment.bytes))) {
        return yield* new ColdStorageReadBackError({ id: attachment.attachmentId });
      }
    }
  });

  const put = Effect.fn("ColdStorageStore.put")(
    function* (
      items: ReadonlyArray<ColdStoredTurnItem>,
      attachments: ReadonlyArray<ColdStoredAttachment> = [],
    ) {
      const movedAt = yield* Clock.currentTimeMillis;
      // Compress on the thread pool before taking the connection.
      const outputs = yield* Effect.forEach(items, (item) =>
        item.output === undefined ? Effect.undefined : compress(item.output),
      );
      const files = yield* Effect.forEach(attachments, (attachment) => compress(attachment.bytes));
      yield* sql.withTransaction(
        Effect.gen(function* () {
          for (const [index, item] of items.entries()) {
            const output = outputs[index];
            if (output !== undefined) {
              yield* sql`
                INSERT OR REPLACE INTO outputs (turn_item_id, thread_id, moved_at, output)
                VALUES (${item.turnItemId}, ${item.threadId}, ${movedAt}, ${output})`;
            }
            if (item.images === undefined) continue;
            yield* sql`DELETE FROM images WHERE turn_item_id = ${item.turnItemId}`;
            for (const image of item.images) {
              yield* sql`
                INSERT INTO images (turn_item_id, thread_id, image_index, media_type, bytes)
                VALUES (${item.turnItemId}, ${item.threadId}, ${image.index}, ${image.mimeType},
                  ${Buffer.from(image.data, "base64")})`;
            }
          }
          for (const [index, attachment] of attachments.entries()) {
            yield* sql`
              INSERT OR REPLACE INTO attachments
                (attachment_id, thread_id, moved_at, file_name, bytes)
              VALUES (${attachment.attachmentId}, ${attachment.threadId}, ${movedAt},
                ${attachment.fileName}, ${files[index] ?? new Uint8Array()})`;
          }
          yield* readBack(items, attachments);
        }),
      );
      return true;
    },
    Effect.catch((cause) =>
      Effect.logWarning("Cold storage write failed", { cause }).pipe(Effect.as(false)),
    ),
  );

  const restore = (item: OrchestrationV2TurnItem): Effect.Effect<OrchestrationV2TurnItem> => {
    if (item.type === "command_execution" && item.fromColdStorage === true) {
      if (item.output !== undefined) return Effect.succeed(item);
      return sql<{ readonly output: Uint8Array }>`
        SELECT output FROM outputs WHERE turn_item_id = ${item.id}`.pipe(
        Effect.flatMap(([row]) =>
          row === undefined
            ? Effect.succeed(item)
            : decompress(row.output).pipe(
                Effect.map((output) => ({ ...item, output: output.toString("utf8") })),
              ),
        ),
        Effect.catch((cause) =>
          Effect.logWarning("Cold storage read failed", { cause }).pipe(Effect.as(item)),
        ),
        Effect.withSpan("ColdStorageStore.restore"),
      );
    }
    if (item.type === "dynamic_tool" && item.fromColdStorage === true) {
      return sql<{
        readonly image_index: number;
        readonly media_type: string;
        readonly bytes: Uint8Array;
      }>`
        SELECT image_index, media_type, bytes FROM images
        WHERE turn_item_id = ${item.id} ORDER BY image_index`.pipe(
        Effect.map((rows) =>
          rows.length === 0
            ? item
            : {
                ...item,
                output: putToolOutputImageData(
                  item.output,
                  rows.map((row) => ({
                    index: row.image_index,
                    mimeType: row.media_type,
                    data: Buffer.from(row.bytes).toString("base64"),
                  })),
                ),
              },
        ),
        Effect.catch((cause) =>
          Effect.logWarning("Cold storage read failed", { cause }).pipe(Effect.as(item)),
        ),
        Effect.withSpan("ColdStorageStore.restore"),
      );
    }
    return Effect.succeed(item);
  };

  const attachmentFile = Effect.fn("ColdStorageStore.attachmentFile")(
    function* (attachmentId: string) {
      const existing = resolveAttachmentPathById({ attachmentsDir, attachmentId });
      if (existing !== null) return existing;
      const [row] = yield* sql<{ readonly file_name: string; readonly bytes: Uint8Array }>`
        SELECT file_name, bytes FROM attachments WHERE attachment_id = ${attachmentId}`;
      if (row === undefined) return null;
      const target = resolveAttachmentRelativePath({
        attachmentsDir,
        relativePath: row.file_name,
      });
      if (target === null) return null;
      const bytes = yield* decompress(row.bytes);
      yield* fs.makeDirectory(attachmentsDir, { recursive: true });
      // The stale-upload sweep removes a `.part` file a stop leaves behind.
      const partial = `${target}.part`;
      yield* fs.writeFile(partial, bytes);
      yield* fs.rename(partial, target);
      return target;
    },
    Effect.catch((cause) =>
      Effect.logWarning("Cold storage attachment read failed", { cause }).pipe(Effect.as(null)),
    ),
  );

  const movedThreads = sql<{ readonly thread_id: string; readonly updated_at: string }>`
    SELECT thread_id, updated_at FROM threads`.pipe(
    Effect.map((rows) => new Map(rows.map((row) => [row.thread_id, row.updated_at]))),
    Effect.catch((cause) =>
      Effect.logWarning("Cold storage read failed", { cause }).pipe(
        Effect.as(new Map<string, string>()),
      ),
    ),
  );

  const markMoved = (threadId: string, updatedAt: string) =>
    sql`
      INSERT OR REPLACE INTO threads (thread_id, updated_at) VALUES (${threadId}, ${updatedAt})`.pipe(
      Effect.catch((cause) => Effect.logWarning("Cold storage write failed", { cause })),
      Effect.asVoid,
    );

  const size = Effect.gen(function* () {
    const [pages] = yield* sql<{ readonly page_count: number }>`PRAGMA page_count`;
    const [pageSize] = yield* sql<{ readonly page_size: number }>`PRAGMA page_size`;
    return (pages?.page_count ?? 0) * (pageSize?.page_size ?? 0);
  }).pipe(Effect.orElseSucceed(() => undefined));

  return ColdStorageStore.of({
    put,
    restore,
    attachmentFile,
    movedThreads,
    markMoved,
    size,
  });
});

// A damaged cold storage must not stop the server; moved items then show without their output.
export const layer = Layer.effect(
  ColdStorageStore,
  Effect.gen(function* () {
    const { attachmentsDir } = yield* ServerConfig.ServerConfig;
    return yield* open.pipe(
      Effect.catch((cause) =>
        Effect.logWarning("Cold storage unavailable", { cause }).pipe(
          Effect.as(unavailable(attachmentsDir)),
        ),
      ),
    );
  }),
);
