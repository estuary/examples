
// Generated for published documents of derived collection Dani/demo-ads/ad-clicks-by-platform.
export type Document = /* A document that represents the click count for each ad platform */ {
    click_count?: number;
    platform: string;
};


// Generated for read documents of sourced collection Dani/demo-ads/ad_impressions.
export type SourceFromImpressions = {
    "_meta": {
        before?: /* Record state immediately before this change was applied. */ {
            ad_id?: /* (source type: non-nullable int4) */ number;
            campaign_id?: /* (source type: non-nullable int4) */ number;
            country?: /* (source type: varchar) */ string | null;
            device_type?: /* (source type: varchar) */ string | null;
            impression_id: /* (source type: non-nullable int4) */ number;
            impression_timestamp?: /* (source type: timestamptz) */ string | null;
            platform?: /* (source type: non-nullable varchar) */ string;
            user_id?: /* (source type: varchar) */ string | null;
        };
        op: /* Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete. */ "c" | "d" | "u";
        source: {
            loc: /* Location of this WAL event as [last Commit.EndLSN; event LSN; current Begin.FinalLSN]. See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html */ number[];
            schema: /* Database schema (namespace) of the event. */ string;
            snapshot?: /* Snapshot is true if the record was produced from an initial table backfill and unset if produced from the replication log. */ boolean;
            table: /* Database table of the event. */ string;
            ts_ms?: /* Unix timestamp (in millis) at which this event was recorded by the database. */ number;
            txid?: /* The 32-bit transaction ID assigned by Postgres to the commit which produced this change. */ number;
        };
    };
    ad_id?: /* (source type: non-nullable int4) */ number;
    campaign_id?: /* (source type: non-nullable int4) */ number;
    country?: /* (source type: varchar) */ string | null;
    device_type?: /* (source type: varchar) */ string | null;
    impression_id: /* (source type: non-nullable int4) */ number;
    impression_timestamp?: /* (source type: timestamptz) */ string | null;
    platform?: /* (source type: non-nullable varchar) */ string;
    user_id?: /* (source type: varchar) */ string | null;
};


// Generated for schema $anchor PublicAd_impressions."
export type SourceFromImpressionsPublicAd_impressions = {
    ad_id?: /* (source type: non-nullable int4) */ number;
    campaign_id?: /* (source type: non-nullable int4) */ number;
    country?: /* (source type: varchar) */ string | null;
    device_type?: /* (source type: varchar) */ string | null;
    impression_id: /* (source type: non-nullable int4) */ number;
    impression_timestamp?: /* (source type: timestamptz) */ string | null;
    platform?: /* (source type: non-nullable varchar) */ string;
    user_id?: /* (source type: varchar) */ string | null;
};


// Generated for read documents of sourced collection Dani/demo-ads/ad_clicks.
export type SourceFromClicks = {
    "_meta": {
        before?: /* Record state immediately before this change was applied. */ {
            ad_id?: /* (source type: non-nullable int4) */ number;
            campaign_id?: /* (source type: non-nullable int4) */ number;
            click_id: /* (source type: non-nullable int4) */ number;
            click_timestamp?: /* (source type: timestamptz) */ string | null;
            conversion_flag?: /* (source type: bool) */ boolean | null;
            impression_id?: /* (source type: int4) */ number | null;
            landing_page_url?: /* (source type: varchar) */ string | null;
            platform?: /* (source type: non-nullable varchar) */ string;
            user_id?: /* (source type: varchar) */ string | null;
        };
        op: /* Change operation type: 'c' Create/Insert, 'u' Update, 'd' Delete. */ "c" | "d" | "u";
        source: {
            loc: /* Location of this WAL event as [last Commit.EndLSN; event LSN; current Begin.FinalLSN]. See https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html */ number[];
            schema: /* Database schema (namespace) of the event. */ string;
            snapshot?: /* Snapshot is true if the record was produced from an initial table backfill and unset if produced from the replication log. */ boolean;
            table: /* Database table of the event. */ string;
            ts_ms?: /* Unix timestamp (in millis) at which this event was recorded by the database. */ number;
            txid?: /* The 32-bit transaction ID assigned by Postgres to the commit which produced this change. */ number;
        };
    };
    ad_id?: /* (source type: non-nullable int4) */ number;
    campaign_id?: /* (source type: non-nullable int4) */ number;
    click_id: /* (source type: non-nullable int4) */ number;
    click_timestamp?: /* (source type: timestamptz) */ string | null;
    conversion_flag?: /* (source type: bool) */ boolean | null;
    impression_id?: /* (source type: int4) */ number | null;
    landing_page_url?: /* (source type: varchar) */ string | null;
    platform?: /* (source type: non-nullable varchar) */ string;
    user_id?: /* (source type: varchar) */ string | null;
};


// Generated for schema $anchor PublicAd_clicks."
export type SourceFromClicksPublicAd_clicks = {
    ad_id?: /* (source type: non-nullable int4) */ number;
    campaign_id?: /* (source type: non-nullable int4) */ number;
    click_id: /* (source type: non-nullable int4) */ number;
    click_timestamp?: /* (source type: timestamptz) */ string | null;
    conversion_flag?: /* (source type: bool) */ boolean | null;
    impression_id?: /* (source type: int4) */ number | null;
    landing_page_url?: /* (source type: varchar) */ string | null;
    platform?: /* (source type: non-nullable varchar) */ string;
    user_id?: /* (source type: varchar) */ string | null;
};


export abstract class IDerivation {
    // Construct a new Derivation instance from a Request.Open message.
    constructor(_open: { state: unknown }) { }

    // flush awaits any remaining documents to be published and returns them.
    // deno-lint-ignore require-await
    async flush(): Promise<Document[]> {
        return [];
    }

    // reset is called only when running catalog tests, and must reset any internal state.
    async reset() { }

    // startCommit is notified of a runtime commit in progress, and returns an optional
    // connector state update to be committed.
    startCommit(_startCommit: { runtimeCheckpoint: unknown }): { state?: { updated: unknown, mergePatch: boolean } } {
        return {};
    }

    abstract fromImpressions(read: { doc: SourceFromImpressions }): Document[];
    abstract fromClicks(read: { doc: SourceFromClicks }): Document[];
}
