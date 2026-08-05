import { describe, expect, it, vi } from "vitest";
import type { RemoteDBSettings } from "@lib/common/types.ts";
import { LiveSyncCouchDBReplicator } from "./LiveSyncReplicator.ts";

describe("LiveSyncCouchDBReplicator continuous catch-up", () => {
    it("starts a fresh continuous task when a newer request joined one that was closing", async () => {
        let releaseFirstCatchUp: (value: boolean) => void = () => {};
        const firstCatchUp = new Promise<boolean>((resolve) => {
            releaseFirstCatchUp = resolve;
        });
        const runFiniteReplicationActivity = vi.fn(async <T>(task: () => Promise<T>) => await task());
        const replicator = Object.create(LiveSyncCouchDBReplicator.prototype) as LiveSyncCouchDBReplicator;
        replicator.env = {
            services: {
                database: {
                    localDatabase: {
                        localDatabase: {},
                    },
                },
                replicator: {
                    runFiniteReplicationActivity,
                },
            },
        } as unknown as LiveSyncCouchDBReplicator["env"];
        const catchUp = vi
            .spyOn(replicator, "openOneShotReplication")
            .mockReturnValueOnce(firstCatchUp)
            .mockResolvedValueOnce(false);
        const setting = {} as RemoteDBSettings;

        const firstRequest = replicator.openContinuousReplication(setting, false, false);
        await vi.waitFor(() => expect(catchUp).toHaveBeenCalledOnce());
        const newerRequest = replicator.openContinuousReplication(setting, false, false);
        releaseFirstCatchUp(false);

        await expect(firstRequest).resolves.toBe(false);
        await expect(newerRequest).resolves.toBe(false);
        expect(catchUp).toHaveBeenCalledTimes(2);
    });

    it("does not abort a replacement replication when the previous process settles", async () => {
        const syncHandler = {
            on: vi.fn().mockReturnThis(),
            then: vi.fn(() => new Promise<void>(() => {})),
            cancel: vi.fn(),
        };
        const replicator = Object.create(LiveSyncCouchDBReplicator.prototype) as LiveSyncCouchDBReplicator;
        const processing = replicator.processSync(syncHandler as never, false, 0, 0, "sync", false);
        const previousController = replicator.controller;
        expect(previousController).toBeDefined();

        previousController!.abort();
        const replacementController = new AbortController();
        replicator.controller = replacementController;

        await expect(processing).resolves.toBe("DONE");
        expect(replacementController.signal.aborted).toBe(false);
        expect(replicator.controller).toBe(replacementController);
    });

    it("exposes the initial pull-only catch-up as finite replication activity", async () => {
        const runFiniteReplicationActivity = vi.fn(async <T>(task: () => Promise<T>) => await task());
        const replicator = Object.create(LiveSyncCouchDBReplicator.prototype) as LiveSyncCouchDBReplicator;
        replicator.env = {
            services: {
                database: {
                    localDatabase: {
                        localDatabase: {},
                    },
                },
                replicator: {
                    runFiniteReplicationActivity,
                },
            },
        } as unknown as LiveSyncCouchDBReplicator["env"];
        const catchUp = vi.spyOn(replicator, "openOneShotReplication").mockResolvedValue(false);
        const setting = {} as RemoteDBSettings;

        await expect(replicator.openContinuousReplication(setting, false, false)).resolves.toBe(false);

        expect(runFiniteReplicationActivity).toHaveBeenCalledOnce();
        expect(runFiniteReplicationActivity).toHaveBeenCalledWith(expect.any(Function), { label: "replication" });
        expect(catchUp).toHaveBeenCalledWith(setting, false, false, "pullOnly");
    });

    it("starts another finite catch-up when the live channel retries with smaller batches", async () => {
        const runFiniteReplicationActivity = vi.fn(async <T>(task: () => Promise<T>) => await task());
        const localDatabase = {
            info: vi.fn().mockResolvedValue({ update_seq: 7 }),
            sync: vi.fn(() => ({})),
        };
        const replicator = Object.create(LiveSyncCouchDBReplicator.prototype) as LiveSyncCouchDBReplicator;
        replicator.env = {
            services: {
                database: {
                    localDatabase: { localDatabase },
                },
                replicator: { runFiniteReplicationActivity },
            },
        } as unknown as LiveSyncCouchDBReplicator["env"];
        replicator.docArrived = 0;
        replicator.docSent = 0;
        replicator.updateInfo = vi.fn();
        replicator.terminateSync = vi.fn();
        const catchUp = vi
            .spyOn(replicator, "openOneShotReplication")
            .mockResolvedValueOnce(true)
            .mockResolvedValueOnce(false);
        vi.spyOn(replicator, "checkReplicationConnectivity").mockResolvedValue({
            db: {},
            info: { update_seq: 9 },
            syncOption: {},
        } as never);
        vi.spyOn(replicator, "processSync").mockResolvedValue("NEED_RETRY");
        const setting = {
            batch_size: 20,
            batches_limit: 20,
        } as RemoteDBSettings;

        await expect(replicator.openContinuousReplication(setting, false, false)).resolves.toBe(false);

        expect(runFiniteReplicationActivity).toHaveBeenCalledTimes(2);
        expect(catchUp).toHaveBeenNthCalledWith(1, setting, false, false, "pullOnly");
        expect(catchUp).toHaveBeenNthCalledWith(
            2,
            expect.objectContaining({ batch_size: 12, batches_limit: 12 }),
            false,
            false,
            "pullOnly"
        );
    });

    it("uses a pull-only live replication handler for the headless inbound channel", async () => {
        const runFiniteReplicationActivity = vi.fn(async <T>(task: () => Promise<T>) => await task());
        const pullHandler = {};
        const localDatabase = {
            info: vi.fn().mockResolvedValue({ update_seq: 7 }),
            replicate: {
                from: vi.fn(() => pullHandler),
            },
            sync: vi.fn(),
        };
        const replicator = Object.create(LiveSyncCouchDBReplicator.prototype) as LiveSyncCouchDBReplicator;
        replicator.env = {
            services: {
                database: {
                    localDatabase: { localDatabase },
                },
                replicator: { runFiniteReplicationActivity },
            },
        } as unknown as LiveSyncCouchDBReplicator["env"];
        replicator.docArrived = 0;
        replicator.docSent = 0;
        replicator.updateInfo = vi.fn();
        replicator.terminateSync = vi.fn();
        vi.spyOn(replicator, "openOneShotReplication").mockResolvedValue(true);
        vi.spyOn(replicator, "checkReplicationConnectivity").mockResolvedValue({
            db: { name: "remote" },
            info: { update_seq: 9 },
            syncOption: { live: true, retry: true, heartbeat: 30000 },
        } as never);
        const processSync = vi.spyOn(replicator, "processSync").mockResolvedValue("DONE");
        const setting = {
            batch_size: 20,
            batches_limit: 20,
            readChunksOnline: false,
        } as RemoteDBSettings;

        await expect(replicator.openContinuousPullReplication(setting, false)).resolves.toBe(true);

        expect(localDatabase.replicate.from).toHaveBeenCalledWith(
            { name: "remote" },
            expect.objectContaining({
                live: true,
                retry: true,
                heartbeat: false,
                timeout: 500,
            })
        );
        expect(localDatabase.sync).not.toHaveBeenCalled();
        expect(processSync).toHaveBeenCalledWith(pullHandler, false, 0, 0, "pullOnly", false);
    });
});
